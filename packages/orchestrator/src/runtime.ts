import type {
  AgentProfile,
  ProviderCredential,
  RunEvent,
  RunNodeState,
  RunRecord,
  SuspendedState,
  WorkflowDefinition,
  WorkflowEdge,
  WorkflowNode,
} from "@agent-studio/shared";
import vm from "vm";
import { createChatModelAdapter, type ChatMessage } from "./providers";

type ExecutionDependencies = {
  workflow: WorkflowDefinition;
  agents: AgentProfile[];
  providers: ProviderCredential[];
  runId: string;
  input: Record<string, string>;
  onEvent: (event: RunEvent) => Promise<void> | void;
  approvedNodeIds?: string[];
  suspendedState?: SuspendedState;
  previousNodes?: RunNodeState[];
};

type ExecutionContext = {
  workflow: WorkflowDefinition;
  nodesById: Map<string, WorkflowNode>;
  incoming: Map<string, WorkflowEdge[]>;
  outgoing: Map<string, WorkflowEdge[]>;
  outputs: Map<string, unknown>;
  runNodes: Map<string, RunNodeState>;
  agentsById: Map<string, AgentProfile>;
  providersById: Map<string, ProviderCredential>;
  input: Record<string, string>;
  onEvent: (event: RunEvent) => Promise<void> | void;
  runId: string;
};

function now() {
  return new Date().toISOString();
}

function eventBase(runId: string, type: RunEvent["type"], nodeId?: string) {
  return {
    id: crypto.randomUUID(),
    runId,
    nodeId,
    type,
    timestamp: now(),
  };
}

function buildMaps(workflow: WorkflowDefinition) {
  const nodesById = new Map(workflow.nodes.map((node) => [node.id, node]));
  const incoming = new Map<string, WorkflowEdge[]>();
  const outgoing = new Map<string, WorkflowEdge[]>();

  for (const node of workflow.nodes) {
    incoming.set(node.id, []);
    outgoing.set(node.id, []);
  }

  for (const edge of workflow.edges) {
    incoming.get(edge.target)?.push(edge);
    outgoing.get(edge.source)?.push(edge);
  }

  return { nodesById, incoming, outgoing };
}

function validateGraph(workflow: WorkflowDefinition) {
  const { incoming, outgoing } = buildMaps(workflow);
  const startNodes = workflow.nodes.filter(
    (node) => (incoming.get(node.id)?.length ?? 0) === 0,
  );
  if (startNodes.length === 0) {
    throw new Error("Workflow must have at least one start node (a node with no incoming connections).");
  }

  const visited = new Set<string>();
  const queue = [...startNodes.map((n) => n.id)];
  while (queue.length > 0) {
    const current = queue.shift()!;
    if (visited.has(current)) {
      continue;
    }
    visited.add(current);
    for (const edge of outgoing.get(current) ?? []) {
      queue.push(edge.target);
    }
  }

  if (visited.size !== workflow.nodes.length) {
    throw new Error("Workflow has unreachable nodes. All nodes must be connected to the execution path.");
  }
}

function buildPrompt(
  node: Extract<WorkflowNode, { type: "agent" }>,
  upstreamOutputs: Array<{ from: string; output: unknown }>,
  input: Record<string, string>,
) {
  const upstreamBlock = upstreamOutputs
    .filter(({ output }) => output !== undefined)
    .map(
      ({ from, output }) =>
        `Upstream node ${from} output:\n${typeof output === "string" ? output : JSON.stringify(output, null, 2)}`,
    )
    .join("\n\n");

  const inputBlock = Object.entries(input)
    .map(([key, value]) => `${key}: ${value}`)
    .join("\n");

  return [node.data.prompt, inputBlock ? `Inputs:\n${inputBlock}` : "", upstreamBlock]
    .filter(Boolean)
    .join("\n\n");
}

function interpolate(template: string, context: Record<string, unknown>) {
  return template.replace(/\{\{(.*?)\}\}/g, (_, key) => {
    const value = context[key.trim()];
    if (value == null) {
      return "";
    }
    return typeof value === "string" ? value : JSON.stringify(value);
  });
}

async function executeNode(
  node: WorkflowNode,
  context: ExecutionContext,
): Promise<{ output?: unknown; failed?: string; route?: string }> {
  const state = context.runNodes.get(node.id)!;
  state.status = "running";
  state.startedAt = now();
  await context.onEvent({
    ...eventBase(context.runId, "started", node.id),
    message: `${node.label} started`,
  });

  const upstreamOutputs = (context.incoming.get(node.id) ?? []).map((edge) => ({
    from: edge.source,
    output: context.outputs.get(edge.source),
  }));

  try {
    switch (node.type) {
      case "input": {
        const variableMap = Object.fromEntries(
          node.data.variables.map((name) => [name, context.input[name] ?? ""]),
        );
        const output = interpolate(node.data.text, variableMap);
        return { output };
      }
      case "agent": {
        const agent = context.agentsById.get(node.data.agentProfileId);
        if (!agent) {
          throw new Error(`Unknown agent profile: ${node.data.agentProfileId}`);
        }
        const provider = context.providersById.get(agent.providerId);
        if (!provider) {
          throw new Error(`Unknown provider: ${agent.providerId}`);
        }
        const adapter = createChatModelAdapter(provider, agent);
        const prompt = buildPrompt(node, upstreamOutputs, context.input);

        const messages: ChatMessage[] = [
          { role: "system", content: agent.systemPrompt },
          { role: "user", content: prompt },
        ];

        let loopCount = 0;
        const maxToolLoops = 5;

        while (loopCount < maxToolLoops) {
          const result = await adapter.generate(
            messages,
            {
              temperature: agent.temperature,
              maxTokens: agent.maxTokens,
              onDelta: async (delta) => {
                await context.onEvent({
                  ...eventBase(context.runId, "stream_delta", node.id),
                  message: delta,
                });
              },
            },
          );

          if (result.toolCalls && result.toolCalls.length > 0) {
            // Append assistant response to messages
            messages.push({
              role: "assistant",
              content: result.text || null,
              tool_calls: result.toolCalls.map((tc) => ({
                id: tc.id,
                type: "function",
                function: { name: tc.name, arguments: tc.arguments },
              })),
            });

            // Execute tool calls
            for (const tc of result.toolCalls) {
              await context.onEvent({
                ...eventBase(context.runId, "started", node.id),
                message: `Executing tool ${tc.name}...`,
              });

              let toolResult = "";
              try {
                if (tc.name === "http_request") {
                  const args = JSON.parse(tc.arguments) as {
                    url: string;
                    method: string;
                    headers?: string;
                    body?: string;
                  };
                  const res = await fetch(args.url, {
                    method: args.method,
                    headers: {
                      "Content-Type": "application/json",
                      ...(args.headers ? JSON.parse(args.headers) : {}),
                    },
                    body: args.method === "POST" ? args.body : undefined,
                  });
                  toolResult = await res.text();
                } else if (tc.name === "delegate_to_agent") {
                  const args = JSON.parse(tc.arguments) as {
                    agentId: string;
                    prompt: string;
                  };
                  const targetAgent = context.agentsById.get(args.agentId);
                  if (!targetAgent) {
                    throw new Error(`Target agent ${args.agentId} not found.`);
                  }
                  const targetProvider = context.providersById.get(targetAgent.providerId);
                  if (!targetProvider) {
                    throw new Error(`Provider for target agent ${args.agentId} not found.`);
                  }
                  const targetAdapter = createChatModelAdapter(targetProvider, targetAgent);

                  // Stream status update to UI
                  await context.onEvent({
                    ...eventBase(context.runId, "started", node.id),
                    message: `Delegating to agent ${targetAgent.name}: "${args.prompt}"`,
                  });

                  const res = await targetAdapter.generate(
                    [
                      { role: "system", content: targetAgent.systemPrompt },
                      { role: "user", content: args.prompt },
                    ],
                    {
                      temperature: targetAgent.temperature,
                      maxTokens: targetAgent.maxTokens,
                    },
                  );
                  toolResult = res.text;
                } else {
                  throw new Error(`Unknown tool: ${tc.name}`);
                }
              } catch (err) {
                toolResult = `Error: ${err instanceof Error ? err.message : String(err)}`;
              }

              await context.onEvent({
                ...eventBase(context.runId, "completed", node.id),
                message: `Tool ${tc.name} completed.`,
              });

              messages.push({
                role: "tool",
                tool_call_id: tc.id,
                name: tc.name,
                content: toolResult,
              });
            }

            loopCount++;
          } else {
            return { output: result.text };
          }
        }

        throw new Error(`Agent Node exceeded maximum tool calling loops (${maxToolLoops})`);
      }
      case "router": {
        const rawInput = upstreamOutputs
          .filter(({ output }) => output !== undefined)
          .map((item) => item.output)
          .join("\n");
        try {
          const parsed = JSON.parse(String(rawInput)) as { route?: string };
          return { output: parsed, route: parsed.route ?? node.data.defaultRoute };
        } catch {
          return {
            output: { route: node.data.defaultRoute, raw: rawInput },
            route: node.data.defaultRoute,
          };
        }
      }
      case "http_tool": {
        const body = node.data.bodyTemplate
          ? interpolate(
              node.data.bodyTemplate,
              Object.fromEntries(
                upstreamOutputs
                  .filter(({ output }) => output !== undefined)
                  .map((item) => [item.from, item.output]),
              ),
            )
          : undefined;
        const response = await fetch(node.data.url, {
          method: node.data.method,
          headers: {
            "Content-Type": "application/json",
            ...node.data.headers,
          },
          body: node.data.method === "POST" ? body : undefined,
        });
        const text = await response.text();
        if (!response.ok) {
          throw new Error(`HTTP tool failed (${response.status}): ${text}`);
        }
        return { output: text };
      }
      case "output": {
        const payload = upstreamOutputs
          .filter(({ output }) => output !== undefined)
          .map(({ from, output }) => `## ${from}\n${typeof output === "string" ? output : JSON.stringify(output, null, 2)}`)
          .join("\n\n");
        const output = node.data.template
          ? `${node.data.template}\n\n${payload}`
          : payload;
        return { output };
      }
      case "code": {
        const inputVariables = Object.fromEntries(
          upstreamOutputs
            .filter(({ output }) => output !== undefined)
            .map((item) => [item.from, item.output]),
        );

        const sandbox = {
          inputs: inputVariables,
          globalInputs: context.input,
          console: {
            log: (...args: unknown[]) => console.log("[code node console]:", ...args),
          },
        };

        const scriptCode = `(async () => {
          ${node.data.code}
        })()`;
        
        try {
          const result = await vm.runInNewContext(scriptCode, sandbox, { timeout: 5000 });
          return { output: result };
        } catch (err) {
          throw new Error(`Code execution error: ${err instanceof Error ? err.message : String(err)}`);
        }
      }
      case "group_chat": {
        const inputVariables = Object.fromEntries(
          upstreamOutputs
            .filter(({ output }) => output !== undefined)
            .map((item) => [item.from, item.output]),
        );

        const initialPrompt = interpolate(node.data.prompt, {
          input: context.input.user_goal ?? "",
          ...inputVariables,
        });

        const participantIds = node.data.agentProfileIds;
        if (!participantIds || participantIds.length === 0) {
          throw new Error("Group Chat node requires at least one participant agent.");
        }

        const participants = participantIds
          .map((id) => context.agentsById.get(id))
          .filter(Boolean) as AgentProfile[];

        if (participants.length === 0) {
          throw new Error("None of the participant agent profiles were found.");
        }

        const chatHistory: { sender: string; content: string }[] = [
          { sender: "User", content: initialPrompt },
        ];

        await context.onEvent({
          ...eventBase(context.runId, "stream_delta", node.id),
          message: `[System]: Starting Group Chat conversation...\nTopic: ${initialPrompt}\n\n`,
        });

        let currentTurn = 0;
        const maxTurns = node.data.maxTurns || 5;
        const terminationWord = node.data.terminationCondition || "TERMINATE";
        let lastResponse = "";

        while (currentTurn < maxTurns) {
          // Select next speaker
          let chosenAgent = participants[currentTurn % participants.length];

          if (node.data.speakerSelection === "auto" && participants.length > 1) {
            const supervisor = participants[0];
            const supervisorProvider = context.providersById.get(supervisor.providerId);
            if (supervisorProvider) {
              const supervisorAdapter = createChatModelAdapter(supervisorProvider, supervisor);
              const historyText = chatHistory
                .map((msg) => `${msg.sender}: ${msg.content}`)
                .join("\n");
              const selectionPrompt = `Given this group chat history:\n${historyText}\n\nChoose the next speaker from this list: [${participants.map(p => p.name).join(", ")}]. Output ONLY the exact name of the selected agent.`;
              try {
                const choiceRes = await supervisorAdapter.generate(
                  [
                    { role: "system", content: "You are a group chat moderator. Select the next speaker name." },
                    { role: "user", content: selectionPrompt }
                  ],
                  { temperature: 0.1, maxTokens: 10 }
                );
                const chosenName = choiceRes.text.trim();
                const matched = participants.find(p => p.name.toLowerCase() === chosenName.toLowerCase());
                if (matched) {
                  chosenAgent = matched;
                }
              } catch (e) {
                // Fallback to round-robin on error
              }
            }
          }

          const provider = context.providersById.get(chosenAgent.providerId);
          if (!provider) {
            throw new Error(`Provider not found for agent ${chosenAgent.name}`);
          }

          const adapter = createChatModelAdapter(provider, chosenAgent);
          const historyText = chatHistory
            .map((msg) => `${msg.sender}: ${msg.content}`)
            .join("\n\n");

          const prompt = `You are participating in a multi-agent group chat.\n\nYour profile:\n- Name: ${chosenAgent.name}\n- Instructions: ${chosenAgent.systemPrompt}\n\nConversation history:\n${historyText}\n\nFormulate your next response. Speak directly and naturally. Do NOT prefix your response with your name.`;

          await context.onEvent({
            ...eventBase(context.runId, "stream_delta", node.id),
            message: `\n[${chosenAgent.name}]: `,
          });

          const result = await adapter.generate(
            [
              { role: "system", content: chosenAgent.systemPrompt },
              { role: "user", content: prompt }
            ],
            {
              temperature: chosenAgent.temperature,
              maxTokens: chosenAgent.maxTokens,
              onDelta: async (delta) => {
                await context.onEvent({
                  ...eventBase(context.runId, "stream_delta", node.id),
                  message: delta,
                });
              },
            }
          );

          lastResponse = result.text;
          chatHistory.push({ sender: chosenAgent.name, content: lastResponse });

          if (lastResponse.toUpperCase().includes(terminationWord.toUpperCase())) {
            await context.onEvent({
              ...eventBase(context.runId, "stream_delta", node.id),
              message: `\n\n[System]: Termination condition reached ("${terminationWord}"). Ending conversation.`,
            });
            break;
          }

          currentTurn++;
        }

        const finalTranscript = chatHistory
          .map((msg) => `${msg.sender}: ${msg.content}`)
          .join("\n\n");

        return { output: finalTranscript };
      }
    }
  } catch (error) {
    return {
      failed: error instanceof Error ? error.message : "Unknown execution error",
    };
  }
}

export async function executeWorkflow(
  dependencies: ExecutionDependencies,
): Promise<RunRecord> {
  validateGraph(dependencies.workflow);

  const { nodesById, incoming, outgoing } = buildMaps(dependencies.workflow);
  const runNodes = new Map<string, RunNodeState>(
    dependencies.workflow.nodes.map((node) => {
      const prev = dependencies.previousNodes?.find((n) => n.nodeId === node.id);
      return [
        node.id,
        prev || {
          nodeId: node.id,
          status: "idle",
        },
      ];
    }),
  );

  const context: ExecutionContext = {
    workflow: dependencies.workflow,
    nodesById,
    incoming,
    outgoing,
    outputs: new Map(),
    runNodes,
    agentsById: new Map(dependencies.agents.map((agent) => [agent.id, agent])),
    providersById: new Map(
      dependencies.providers.map((provider) => [provider.id, provider]),
    ),
    input: dependencies.input,
    onEvent: dependencies.onEvent,
    runId: dependencies.runId,
  };

  // Restore state if resuming
  const outputs = dependencies.suspendedState
    ? new Map(Object.entries(dependencies.suspendedState.outputs))
    : new Map<string, unknown>();

  // Restore context outputs
  for (const [nodeId, output] of outputs.entries()) {
    context.outputs.set(nodeId, output);
  }

  const executionCounts = dependencies.suspendedState
    ? new Map(Object.entries(dependencies.suspendedState.executionCounts).map(([k, v]) => [k, Number(v)]))
    : new Map<string, number>();

  const routeSelections = dependencies.suspendedState
    ? new Map(Object.entries(dependencies.suspendedState.routeSelections))
    : new Map<string, string>();

  const startNodes = dependencies.workflow.nodes.filter(
    (node) => (incoming.get(node.id)?.length ?? 0) === 0,
  );

  const ready = dependencies.suspendedState
    ? new Set<string>(dependencies.suspendedState.readyNodeIds)
    : new Set<string>(startNodes.map((node) => node.id));

  const approvedNodeIds = new Set<string>([
    ...(dependencies.suspendedState?.approvedNodeIds ?? []),
    ...(dependencies.approvedNodeIds ?? []),
  ]);

  let failed = false;
  let paused = false;
  let finalOutput: unknown;
  let totalExecutions = Array.from(executionCounts.values()).reduce((a, b) => a + b, 0);
  const maxTotalExecutions = 50;
  const maxExecutionsPerNode = 15;

  while (ready.size > 0 && !failed && !paused) {
    const batch = Array.from(ready);
    ready.clear();

    // Check Human-in-the-Loop approval interrupt
    const nodesRequiringApproval = batch.filter((nodeId) => {
      const node = nodesById.get(nodeId);
      return node?.requireApproval && !approvedNodeIds.has(nodeId);
    });

    if (nodesRequiringApproval.length > 0) {
      paused = true;
      // Re-add the batch back to ready so they execute on resume
      for (const nodeId of batch) {
        ready.add(nodeId);
      }

      // Prepare suspended state
      const suspendedState: SuspendedState = {
        readyNodeIds: Array.from(ready),
        outputs: Object.fromEntries(outputs.entries()),
        executionCounts: Object.fromEntries(executionCounts.entries()),
        routeSelections: Object.fromEntries(routeSelections.entries()),
        approvedNodeIds: Array.from(approvedNodeIds),
      };

      // Emit paused event
      for (const nodeId of nodesRequiringApproval) {
        const node = nodesById.get(nodeId)!;
        await dependencies.onEvent({
          ...eventBase(dependencies.runId, "paused", nodeId),
          message: `${node.label} paused: waiting for human approval`,
        });
      }
      break;
    }

    // Check loop guards and queue status
    for (const nodeId of batch) {
      if (totalExecutions >= maxTotalExecutions) {
        failed = true;
        const msg = `Loop limit exceeded: run reached maximum total executions (${maxTotalExecutions}).`;
        const state = runNodes.get(nodeId)!;
        state.status = "failed";
        state.error = msg;
        state.completedAt = now();
        await dependencies.onEvent({
          ...eventBase(dependencies.runId, "failed", nodeId),
          message: msg,
        });
        break;
      }

      const nodeCount = (executionCounts.get(nodeId) ?? 0) + 1;
      if (nodeCount > maxExecutionsPerNode) {
        failed = true;
        const msg = `Loop limit exceeded: node '${nodeId}' exceeded maximum execution limit of ${maxExecutionsPerNode}.`;
        const state = runNodes.get(nodeId)!;
        state.status = "failed";
        state.error = msg;
        state.completedAt = now();
        await dependencies.onEvent({
          ...eventBase(dependencies.runId, "failed", nodeId),
          message: msg,
        });
        break;
      }

      executionCounts.set(nodeId, nodeCount);
      totalExecutions++;

      const state = runNodes.get(nodeId)!;
      state.status = "queued";
      await dependencies.onEvent({
        ...eventBase(dependencies.runId, "queued", nodeId),
        message: `${nodesById.get(nodeId)!.label} queued`,
      });
    }

    if (failed) {
      break;
    }

    // Execute batch
    await Promise.all(
      batch.map(async (nodeId) => {
        const node = nodesById.get(nodeId)!;
        const state = runNodes.get(nodeId)!;

        const result = await executeNode(node, context);

        if (result.failed) {
          state.status = "failed";
          state.error = result.failed;
          state.completedAt = now();
          failed = true;
          await dependencies.onEvent({
            ...eventBase(dependencies.runId, "failed", nodeId),
            message: result.failed,
          });
          return;
        }

        state.status = "completed";
        state.output = result.output;
        state.completedAt = now();
        context.outputs.set(nodeId, result.output);
        outputs.set(nodeId, result.output); // Sync with local outputs map
        if (node.type === "output") {
          finalOutput = result.output;
        }
        if (node.type === "router") {
          routeSelections.set(node.id, result.route ?? node.data.defaultRoute);
        }

        await dependencies.onEvent({
          ...eventBase(dependencies.runId, "completed", nodeId),
          message: `${node.label} completed`,
          output: result.output,
        });

        // Trigger downstream nodes
        for (const edge of outgoing.get(nodeId) ?? []) {
          if (node.type === "router") {
            const route = routeSelections.get(node.id);
            if (edge.label && edge.label !== route) {
              continue;
            }
          }
          ready.add(edge.target);
        }
      }),
    );
  }

  // Determine final run status
  let runStatus: RunRecord["status"] = "completed";
  if (failed) {
    runStatus = "failed";
  } else if (paused) {
    runStatus = "paused";
  }

  // Get suspendedState if paused
  const suspendedState = paused ? {
    readyNodeIds: Array.from(ready),
    outputs: Object.fromEntries(outputs.entries()),
    executionCounts: Object.fromEntries(executionCounts.entries()),
    routeSelections: Object.fromEntries(routeSelections.entries()),
    approvedNodeIds: Array.from(approvedNodeIds),
  } : undefined;

  const run: RunRecord = {
    id: dependencies.runId,
    workflowId: dependencies.workflow.id,
    workflowName: dependencies.workflow.name,
    status: runStatus,
    input: dependencies.input,
    output: finalOutput,
    startedAt: dependencies.suspendedState?.startedAt || now(),
    completedAt: runStatus === "completed" || runStatus === "failed" ? now() : undefined,
    nodes: Array.from(runNodes.values()),
    suspendedState,
  };

  return run;
}
