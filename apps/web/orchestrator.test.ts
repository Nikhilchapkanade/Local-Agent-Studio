import { describe, expect, it, vi } from "vitest";
import { executeWorkflow } from "@agent-studio/orchestrator";
import type {
  AgentProfile,
  ProviderCredential,
  WorkflowDefinition,
} from "@agent-studio/shared";

function streamResponse(parts: string[]) {
  return new Response(
    new ReadableStream({
      start(controller) {
        const encoder = new TextEncoder();
        for (const part of parts) {
          controller.enqueue(encoder.encode(part));
        }
        controller.close();
      },
    }),
  );
}

const now = new Date().toISOString();

const provider: ProviderCredential = {
  id: "provider",
  name: "Compat",
  type: "openai_compatible",
  baseUrl: "https://example.com/v1",
  apiKey: "test-key",
  customHeaders: {},
  defaultModel: "demo-model",
  isDemo: false,
  createdAt: now,
  updatedAt: now,
};

const agents: AgentProfile[] = [
  {
    id: "agent-a",
    name: "Worker A",
    description: "",
    notes: "",
    profileType: "analysis",
    role: "worker",
    providerId: "provider",
    model: "demo-model",
    systemPrompt: "You are worker A.",
    temperature: 0.2,
    maxTokens: 300,
    outputMode: "text",
    allowedTools: [],
    avatar: "",
    isDemo: false,
    createdAt: now,
    updatedAt: now,
  },
  {
    id: "agent-b",
    name: "Worker B",
    description: "",
    notes: "",
    profileType: "implementation",
    role: "worker",
    providerId: "provider",
    model: "demo-model",
    systemPrompt: "You are worker B.",
    temperature: 0.2,
    maxTokens: 300,
    outputMode: "text",
    allowedTools: [],
    avatar: "",
    isDemo: false,
    createdAt: now,
    updatedAt: now,
  },
];

describe("executeWorkflow", () => {
  it("executes a simple branched DAG and emits outputs", async () => {
    const workflow: WorkflowDefinition = {
      id: "wf-1",
      name: "Branch test",
      version: 1,
      description: "",
      createdAt: now,
      updatedAt: now,
      nodes: [
        {
          id: "input",
          type: "input",
          label: "Input",
          description: "",
          position: { x: 0, y: 0 },
          data: { text: "Task: {{goal}}", variables: ["goal"] },
        },
        {
          id: "agent-1",
          type: "agent",
          label: "A",
          description: "",
          position: { x: 100, y: 0 },
          data: { agentProfileId: "agent-a", prompt: "Analyze it." },
        },
        {
          id: "agent-2",
          type: "agent",
          label: "B",
          description: "",
          position: { x: 100, y: 100 },
          data: { agentProfileId: "agent-b", prompt: "Implement it." },
        },
        {
          id: "out",
          type: "output",
          label: "Output",
          description: "",
          position: { x: 200, y: 0 },
          data: { template: "Final synthesis" },
        },
      ],
      edges: [
        { id: "e1", source: "input", target: "agent-1" },
        { id: "e2", source: "input", target: "agent-2" },
        { id: "e3", source: "agent-1", target: "out" },
        { id: "e4", source: "agent-2", target: "out" },
      ],
    };

    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        streamResponse([
          'data: {"choices":[{"delta":{"content":"research"}}]}\n\n',
          "data: [DONE]\n\n",
        ]),
      )
      .mockResolvedValueOnce(
        streamResponse([
          'data: {"choices":[{"delta":{"content":"implementation"}}]}\n\n',
          "data: [DONE]\n\n",
        ]),
      );

    vi.stubGlobal("fetch", fetchMock);

    const events: string[] = [];
    const result = await executeWorkflow({
      workflow,
      agents,
      providers: [provider],
      runId: "run-1",
      input: { goal: "ship an MVP" },
      onEvent: (event) => {
        events.push(event.type);
      },
    });

    expect(result.status).toBe("completed");
    expect(String(result.output)).toContain("research");
    expect(String(result.output)).toContain("implementation");
    expect(events).toContain("stream_delta");
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("executes a cyclic workflow and resolves successfully when router breaks loop", async () => {
    const workflow: WorkflowDefinition = {
      id: "wf-loop",
      name: "Loop test",
      version: 1,
      description: "",
      createdAt: now,
      updatedAt: now,
      nodes: [
        {
          id: "input",
          type: "input",
          label: "Input",
          description: "",
          position: { x: 0, y: 0 },
          data: { text: "Start workflow", variables: [] },
        },
        {
          id: "agent-1",
          type: "agent",
          label: "Agent A",
          description: "",
          position: { x: 100, y: 0 },
          data: { agentProfileId: "agent-a", prompt: "Think" },
        },
        {
          id: "router",
          type: "router",
          label: "Router",
          description: "",
          position: { x: 200, y: 0 },
          data: { defaultRoute: "done" },
        },
        {
          id: "out",
          type: "output",
          label: "Output",
          description: "",
          position: { x: 300, y: 0 },
          data: { template: "End" },
        },
      ],
      edges: [
        { id: "e1", source: "input", target: "agent-1" },
        { id: "e2", source: "agent-1", target: "router" },
        { id: "e3", source: "router", target: "agent-1", label: "loop" },
        { id: "e4", source: "router", target: "out", label: "done" },
      ],
    };

    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        streamResponse([
          'data: {"choices":[{"delta":{"content":"{\\"route\\": \\"loop\\"}"}}]}\n\n',
          "data: [DONE]\n\n",
        ]),
      )
      .mockResolvedValueOnce(
        streamResponse([
          'data: {"choices":[{"delta":{"content":"{\\"route\\": \\"done\\"}"}}]}\n\n',
          "data: [DONE]\n\n",
        ]),
      );

    vi.stubGlobal("fetch", fetchMock);

    const events: string[] = [];
    const result = await executeWorkflow({
      workflow,
      agents,
      providers: [provider],
      runId: "run-2",
      input: {},
      onEvent: (event) => {
        events.push(event.type);
      },
    });

    expect(result.status).toBe("completed");
    expect(fetchMock).toHaveBeenCalledTimes(2);
    // Expected run steps: input -> agent-1 (first) -> router (first) -> agent-1 (second) -> router (second) -> out
    // Total events should contain two completions of agent-1 and router
    const agentCompletions = result.nodes.find(n => n.nodeId === "agent-1");
    expect(agentCompletions?.status).toBe("completed");
  });

  it("fails execution when workflow hits loop protection limit", async () => {
    const workflow: WorkflowDefinition = {
      id: "wf-inf-loop",
      name: "Infinite Loop test",
      version: 1,
      description: "",
      createdAt: now,
      updatedAt: now,
      nodes: [
        {
          id: "input",
          type: "input",
          label: "Input",
          description: "",
          position: { x: 0, y: 0 },
          data: { text: "Start workflow", variables: [] },
        },
        {
          id: "agent-1",
          type: "agent",
          label: "Agent A",
          description: "",
          position: { x: 100, y: 0 },
          data: { agentProfileId: "agent-a", prompt: "Think" },
        },
        {
          id: "router",
          type: "router",
          label: "Router",
          description: "",
          position: { x: 200, y: 0 },
          data: { defaultRoute: "loop" },
        },
      ],
      edges: [
        { id: "e1", source: "input", target: "agent-1" },
        { id: "e2", source: "agent-1", target: "router" },
        { id: "e3", source: "router", target: "agent-1", label: "loop" },
      ],
    };

    // Return infinite loop responses
    const fetchMock = vi.fn().mockImplementation(() =>
      Promise.resolve(
        streamResponse([
          'data: {"choices":[{"delta":{"content":"{\\"route\\": \\"loop\\"}"}}]}\n\n',
          "data: [DONE]\n\n",
        ]),
      ),
    );

    vi.stubGlobal("fetch", fetchMock);

    const result = await executeWorkflow({
      workflow,
      agents,
      providers: [provider],
      runId: "run-3",
      input: {},
      onEvent: () => {},
    });

    expect(result.status).toBe("failed");
    const failedNode = result.nodes.find(n => n.status === "failed");
    expect(failedNode?.error).toContain("Loop limit exceeded");
  });

  it("executes native tool calling loop for http_request and delegate_to_agent and resolves successfully", async () => {
    const agentsWithTools: AgentProfile[] = [
      {
        ...agents[0],
        allowedTools: ["http", "delegation"],
      },
      agents[1],
    ];

    const workflow: WorkflowDefinition = {
      id: "wf-tools",
      name: "Tools test",
      version: 1,
      description: "",
      createdAt: now,
      updatedAt: now,
      nodes: [
        {
          id: "input",
          type: "input",
          label: "Input",
          description: "",
          position: { x: 0, y: 0 },
          data: { text: "Solve task", variables: [] },
        },
        {
          id: "agent-1",
          type: "agent",
          label: "Agent A",
          description: "",
          position: { x: 100, y: 0 },
          data: { agentProfileId: "agent-a", prompt: "Execute tools" },
        },
        {
          id: "out",
          type: "output",
          label: "Output",
          description: "",
          position: { x: 200, y: 0 },
          data: { template: "End" },
        },
      ],
      edges: [
        { id: "e1", source: "input", target: "agent-1" },
        { id: "e2", source: "agent-1", target: "out" },
      ],
    };

    const fetchMock = vi.fn().mockImplementation((url: string, init?: any) => {
      if (url === "https://example.com/api/data") {
        return Promise.resolve(new Response("external API response"));
      }

      const body = JSON.parse(init.body) as {
        model: string;
        messages: any[];
        stream?: boolean;
      };

      if (body.model === "demo-model") {
        if (body.messages.some(m => m.role === "system" && m.content === "You are worker B.")) {
          return Promise.resolve(
            streamResponse([
              'data: {"choices":[{"delta":{"content":"delegated task completed"}}]}\n\n',
              "data: [DONE]\n\n",
            ]),
          );
        }

        const hasToolResponses = body.messages.filter(m => m.role === "tool");
        if (hasToolResponses.length === 0) {
          return Promise.resolve(new Response(JSON.stringify({
            choices: [{
              message: {
                role: "assistant",
                content: null,
                tool_calls: [{
                  id: "call-http",
                  type: "function",
                  function: {
                    name: "http_request",
                    arguments: JSON.stringify({ url: "https://example.com/api/data", method: "GET" })
                  }
                }]
              }
            }]
          })));
        } else if (hasToolResponses.length === 1) {
          return Promise.resolve(new Response(JSON.stringify({
            choices: [{
              message: {
                role: "assistant",
                content: null,
                tool_calls: [{
                  id: "call-delegate",
                  type: "function",
                  function: {
                    name: "delegate_to_agent",
                    arguments: JSON.stringify({ agentId: "agent-b", prompt: "Do task B" })
                  }
                }]
              }
            }]
          })));
        } else {
          const httpResult = body.messages.find(m => m.role === "tool" && m.tool_call_id === "call-http")?.content;
          const delegateResult = body.messages.find(m => m.role === "tool" && m.tool_call_id === "call-delegate")?.content;
          return Promise.resolve(new Response(JSON.stringify({
            choices: [{
              message: {
                role: "assistant",
                content: `All tasks done: ${httpResult} + ${delegateResult}`
              }
            }]
          })));
        }
      }

      return Promise.reject(new Error(`Unexpected fetch URL: ${url}`));
    });

    vi.stubGlobal("fetch", fetchMock);

    const result = await executeWorkflow({
      workflow,
      agents: agentsWithTools,
      providers: [provider],
      runId: "run-4",
      input: {},
      onEvent: () => {},
    });

    expect(result.status).toBe("completed");
    expect(result.output).toContain("All tasks done: external API response + delegated task completed");
  });

  it("pauses execution when a node requires approval and resumes successfully after approval", async () => {
    const workflow: WorkflowDefinition = {
      id: "wf-hitl",
      name: "HITL test",
      version: 1,
      description: "",
      createdAt: now,
      updatedAt: now,
      nodes: [
        {
          id: "input",
          type: "input",
          label: "Input",
          description: "",
          position: { x: 0, y: 0 },
          data: { text: "Start run", variables: [] },
        },
        {
          id: "agent-1",
          type: "agent",
          label: "Agent A",
          description: "",
          position: { x: 100, y: 0 },
          data: { agentProfileId: "agent-a", prompt: "Think" },
          requireApproval: true, // Requires human approval!
        },
        {
          id: "out",
          type: "output",
          label: "Output",
          description: "",
          position: { x: 200, y: 0 },
          data: { template: "Done" },
        },
      ],
      edges: [
        { id: "e1", source: "input", target: "agent-1" },
        { id: "e2", source: "agent-1", target: "out" },
      ],
    };

    const fetchMock = vi.fn().mockImplementation(() =>
      Promise.resolve(
        streamResponse([
          'data: {"choices":[{"delta":{"content":"approved output"}}]}\n\n',
          "data: [DONE]\n\n",
        ]),
      ),
    );

    vi.stubGlobal("fetch", fetchMock);

    const events: string[] = [];
    const result = await executeWorkflow({
      workflow,
      agents,
      providers: [provider],
      runId: "run-5",
      input: {},
      onEvent: (event) => {
        events.push(event.type);
      },
    });

    // 1. Verify that execution pauses before agent-1
    expect(result.status).toBe("paused");
    expect(result.suspendedState).toBeDefined();
    expect(result.suspendedState?.readyNodeIds).toContain("agent-1");
    expect(events).toContain("paused");

    // 2. Resume execution by approving agent-1
    const resumeResult = await executeWorkflow({
      workflow,
      agents,
      providers: [provider],
      runId: "run-5",
      input: {},
      approvedNodeIds: ["agent-1"], // Approve the node!
      suspendedState: result.suspendedState,
      previousNodes: result.nodes,
      onEvent: () => {},
    });

    // 3. Verify that execution completes successfully after resume
    expect(resumeResult.status).toBe("completed");
    expect(resumeResult.output).toContain("approved output");
  });

  it("forks and resumes execution (time-travel debugging) from a specific middle node", async () => {
    const workflow: WorkflowDefinition = {
      id: "wf-debugger",
      name: "Debugger test",
      version: 1,
      description: "",
      createdAt: now,
      updatedAt: now,
      nodes: [
        {
          id: "input",
          type: "input",
          label: "Input",
          description: "",
          position: { x: 0, y: 0 },
          data: { text: "Start debugger", variables: [] },
        },
        {
          id: "agent-1",
          type: "agent",
          label: "Agent 1",
          description: "",
          position: { x: 100, y: 0 },
          data: { agentProfileId: "agent-a", prompt: "Step 1" },
        },
        {
          id: "agent-2",
          type: "agent",
          label: "Agent 2",
          description: "",
          position: { x: 200, y: 0 },
          data: { agentProfileId: "agent-a", prompt: "Step 2" },
        },
        {
          id: "out",
          type: "output",
          label: "Output",
          description: "",
          position: { x: 300, y: 0 },
          data: { template: "Final: {{agent-2}}" },
        },
      ],
      edges: [
        { id: "e1", source: "input", target: "agent-1" },
        { id: "e2", source: "agent-1", target: "agent-2" },
        { id: "e3", source: "agent-2", target: "out" },
      ],
    };

    // First fetch mock returns "initial step 2"
    let callCount = 0;
    const fetchMock = vi.fn().mockImplementation(() => {
      callCount++;
      return Promise.resolve(
        streamResponse([
          `data: {"choices":[{"delta":{"content":"mocked output ${callCount}"}}]}\n\n`,
          "data: [DONE]\n\n",
        ]),
      );
    });

    vi.stubGlobal("fetch", fetchMock);

    // 1. Run the initial complete workflow execution
    const initialResult = await executeWorkflow({
      workflow,
      agents,
      providers: [provider],
      runId: "run-debugger-1",
      input: {},
      onEvent: () => {},
    });

    expect(initialResult.status).toBe("completed");
    expect(initialResult.output).toBe("Final: {{agent-2}}\n\n## agent-2\nmocked output 2");
    expect(callCount).toBe(2); // agent-1 and agent-2 were executed

    // 2. Simulate forking/time-travel resuming from agent-2
    // We update the prompt of agent-2 in the workflow definition (as if the user edited it)
    const modifiedWorkflow = {
      ...workflow,
      nodes: workflow.nodes.map((node) =>
        node.id === "agent-2"
          ? { ...node, data: { ...node.data, prompt: "Modified Step 2" } }
          : node,
      ),
    };

    // Reconstruct the suspendedState for the fork:
    // We reuse outputs of input and agent-1, and clear agent-2 and out outputs
    const forkSuspendedState = {
      readyNodeIds: ["agent-2"], // Start directly at agent-2!
      outputs: {
        input: "Start debugger",
        "agent-1": "mocked output 1", // Cached output from first run
      },
      executionCounts: {
        input: 1,
        "agent-1": 1,
      },
      routeSelections: {},
      approvedNodeIds: [],
    };

    const forkPreviousNodes = [
      { nodeId: "input", status: "completed" as const, output: "Start debugger" },
      { nodeId: "agent-1", status: "completed" as const, output: "mocked output 1" },
      { nodeId: "agent-2", status: "idle" as const }, // Reset to idle
      { nodeId: "out", status: "idle" as const }, // Reset to idle
    ];

    const forkResult = await executeWorkflow({
      workflow: modifiedWorkflow,
      agents,
      providers: [provider],
      runId: "run-debugger-2",
      input: {},
      suspendedState: forkSuspendedState,
      previousNodes: forkPreviousNodes,
      onEvent: () => {},
    });

    // 3. Verify that the fork runs from agent-2 and succeeds
    expect(forkResult.status).toBe("completed");
    expect(forkResult.output).toBe("Final: {{agent-2}}\n\n## agent-2\nmocked output 3");
    expect(callCount).toBe(3); // Only executed agent-2 once more (callCount went from 2 to 3; agent-1 was NOT executed!)
  });

  it("executes a sandboxed JavaScript code node successfully and propagates output", async () => {
    const workflow: WorkflowDefinition = {
      id: "wf-code",
      name: "Code test",
      version: 1,
      description: "",
      createdAt: now,
      updatedAt: now,
      nodes: [
        {
          id: "input",
          type: "input",
          label: "Input",
          description: "",
          position: { x: 0, y: 0 },
          data: { text: "Start run", variables: [] },
        },
        {
          id: "code-1",
          type: "code",
          label: "JS Executor",
          description: "",
          position: { x: 100, y: 0 },
          data: {
            code: `
              const val = inputs['input'];
              return 'Calculated: ' + val.toUpperCase() + ' + 123';
            `,
          },
        },
        {
          id: "out",
          type: "output",
          label: "Output",
          description: "",
          position: { x: 200, y: 0 },
          data: { template: "Final: {{code-1}}" },
        },
      ],
      edges: [
        { id: "e1", source: "input", target: "code-1" },
        { id: "e2", source: "code-1", target: "out" },
      ],
    };

    const result = await executeWorkflow({
      workflow,
      agents: [],
      providers: [],
      runId: "run-code-node",
      input: {},
      onEvent: () => {},
    });

    expect(result.status).toBe("completed");
    expect(result.output).toBe("Final: {{code-1}}\n\n## code-1\nCalculated: START RUN + 123");
  });

  it("runs a multi-agent group chat conversation node successfully", async () => {
    const workflow: WorkflowDefinition = {
      id: "wf-group-chat",
      name: "Group Chat test",
      version: 1,
      description: "",
      createdAt: now,
      updatedAt: now,
      nodes: [
        {
          id: "input",
          type: "input",
          label: "Input",
          description: "",
          position: { x: 0, y: 0 },
          data: { text: "Start run", variables: [] },
        },
        {
          id: "gc-1",
          type: "group_chat",
          label: "Agent Discussion",
          description: "",
          position: { x: 100, y: 0 },
          data: {
            agentProfileIds: ["agent-a", "agent-b"],
            maxTurns: 2,
            terminationCondition: "TERMINATE",
            speakerSelection: "round_robin",
            prompt: "Topic: {{input}}",
          },
        },
        {
          id: "out",
          type: "output",
          label: "Output",
          description: "",
          position: { x: 200, y: 0 },
          data: { template: "Final: {{gc-1}}" },
        },
      ],
      edges: [
        { id: "e1", source: "input", target: "gc-1" },
        { id: "e2", source: "gc-1", target: "out" },
      ],
    };

    const groupChatAgents = [
      {
        id: "agent-a",
        name: "Agent A",
        description: "",
        notes: "",
        profileType: "custom" as const,
        role: "worker" as const,
        providerId: "provider",
        model: "demo-model",
        systemPrompt: "You are Agent A.",
        temperature: 0.7,
        maxTokens: 100,
        allowedTools: [],
        createdAt: now,
        updatedAt: now,
      },
      {
        id: "agent-b",
        name: "Agent B",
        description: "",
        notes: "",
        profileType: "custom" as const,
        role: "worker" as const,
        providerId: "provider",
        model: "demo-model",
        systemPrompt: "You are Agent B.",
        temperature: 0.7,
        maxTokens: 100,
        allowedTools: [],
        createdAt: now,
        updatedAt: now,
      },
    ];

    let turnCount = 0;
    const fetchMock = vi.fn().mockImplementation(() => {
      turnCount++;
      return Promise.resolve(
        streamResponse([
          `data: {"choices":[{"delta":{"content":"mocked turn ${turnCount}"}}]}\n\n`,
          "data: [DONE]\n\n",
        ]),
      );
    });

    vi.stubGlobal("fetch", fetchMock);

    const result = await executeWorkflow({
      workflow,
      agents: groupChatAgents,
      providers: [provider],
      runId: "run-group-chat-node",
      input: {},
      onEvent: () => {},
    });

    expect(result.status).toBe("completed");
    expect(result.output).toContain("User: Topic: Start run");
    expect(result.output).toContain("Agent A: mocked turn 1");
    expect(result.output).toContain("Agent B: mocked turn 2");
    expect(turnCount).toBe(2); // Agent A and Agent B were each invoked once
  });
});
