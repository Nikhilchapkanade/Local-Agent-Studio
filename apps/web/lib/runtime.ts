import { executeWorkflow } from "@agent-studio/orchestrator";
import type { RunEvent, RunRecord, WorkflowDefinition, WorkflowEdge } from "@agent-studio/shared";
import {
  addRunEvent,
  createRun,
  getRun,
  getWorkflow,
  listAgents,
  listProviders,
  updateRun,
} from "./db";

type Listener = (event: RunEvent) => void;

declare global {
  // eslint-disable-next-line no-var
  var __agentStudioRuntime:
    | {
        listeners: Map<string, Set<Listener>>;
      }
    | undefined;
}

function getState() {
  if (!global.__agentStudioRuntime) {
    global.__agentStudioRuntime = {
      listeners: new Map(),
    };
  }
  return global.__agentStudioRuntime;
}

function publish(event: RunEvent) {
  const listeners = getState().listeners.get(event.runId);
  if (!listeners) {
    return;
  }
  for (const listener of listeners) {
    listener(event);
  }
}

export function subscribeToRun(runId: string, listener: Listener) {
  const state = getState();
  const listeners = state.listeners.get(runId) ?? new Set<Listener>();
  listeners.add(listener);
  state.listeners.set(runId, listeners);

  return () => {
    const current = state.listeners.get(runId);
    current?.delete(listener);
    if (current && current.size === 0) {
      state.listeners.delete(runId);
    }
  };
}

export async function startWorkflowRun(
  workflowId: string,
  input: Record<string, string>,
) {
  const workflow = getWorkflow(workflowId);
  if (!workflow) {
    throw new Error(`Workflow ${workflowId} not found.`);
  }

  const runId = crypto.randomUUID();
  const placeholder: RunRecord = {
    id: runId,
    workflowId: workflow.id,
    workflowName: workflow.name,
    status: "running",
    input,
    startedAt: new Date().toISOString(),
    nodes: workflow.nodes.map((node) => ({
      nodeId: node.id,
      status: "idle",
    })),
  };

  createRun(placeholder);

  void executeWorkflow({
    workflow,
    agents: listAgents(),
    providers: listProviders(),
    runId,
    input,
    onEvent: async (event) => {
      addRunEvent(event);
      publish(event);
    },
  })
    .then((result) => {
      updateRun({
        ...placeholder,
        status: result.status,
        output: result.output,
        completedAt: result.completedAt,
        nodes: result.nodes,
        suspendedState: result.suspendedState,
      });
    })
    .catch((error) => {
      updateRun({
        ...placeholder,
        status: "failed",
        completedAt: new Date().toISOString(),
        nodes: placeholder.nodes.map((node) => ({
          ...node,
          status: node.status === "idle" ? "failed" : node.status,
        })),
        output: error instanceof Error ? error.message : "Run failed",
      });
    });

  return getRun(runId);
}

export async function resumeWorkflowRun(
  runId: string,
  approvedNodeId: string,
) {
  const run = getRun(runId);
  if (!run) {
    throw new Error(`Run ${runId} not found.`);
  }

  if (run.status !== "paused" || !run.suspendedState) {
    throw new Error(`Run ${runId} is not in a paused state.`);
  }

  const workflow = getWorkflow(run.workflowId);
  if (!workflow) {
    throw new Error(`Workflow ${run.workflowId} not found.`);
  }

  // Update status back to running
  const updatedRun: RunRecord = {
    ...run,
    status: "running",
  };
  updateRun(updatedRun);

  // Emit start / resume event
  const resumeEvent = {
    id: crypto.randomUUID(),
    runId,
    type: "started" as const,
    message: "Resuming workflow after human approval",
    timestamp: new Date().toISOString(),
  };
  addRunEvent(resumeEvent);
  publish(resumeEvent);

  void executeWorkflow({
    workflow,
    agents: listAgents(),
    providers: listProviders(),
    runId,
    input: run.input,
    approvedNodeIds: [approvedNodeId],
    suspendedState: run.suspendedState,
    previousNodes: run.nodes,
    onEvent: async (event) => {
      addRunEvent(event);
      publish(event);
    },
  })
    .then((result) => {
      updateRun({
        ...run,
        status: result.status,
        output: result.output,
        completedAt: result.completedAt,
        nodes: result.nodes,
        suspendedState: result.suspendedState,
      });
    })
    .catch((error) => {
      updateRun({
        ...run,
        status: "failed",
        completedAt: new Date().toISOString(),
        nodes: run.nodes.map((node) => ({
          ...node,
          status: node.status === "idle" ? "failed" : node.status,
        })),
        output: error instanceof Error ? error.message : "Run failed",
      });
    });

  return getRun(runId);
}

function buildOutgoingMaps(workflow: WorkflowDefinition) {
  const outgoing = new Map<string, WorkflowEdge[]>();
  for (const node of workflow.nodes) {
    outgoing.set(node.id, []);
  }
  for (const edge of workflow.edges) {
    outgoing.get(edge.source)?.push(edge);
  }
  return outgoing;
}

export async function forkWorkflowRun(
  runId: string,
  nodeId: string,
  updatedWorkflow?: WorkflowDefinition,
) {
  const oldRun = getRun(runId);
  if (!oldRun) {
    throw new Error(`Run ${runId} not found.`);
  }

  const workflow = updatedWorkflow || getWorkflow(oldRun.workflowId);
  if (!workflow) {
    throw new Error(`Workflow ${oldRun.workflowId} not found.`);
  }

  const outgoing = buildOutgoingMaps(workflow);

  // Find all descendant nodes starting from nodeId
  const downstreamNodeIds = new Set<string>();
  const queue = [nodeId];
  while (queue.length > 0) {
    const current = queue.shift()!;
    if (downstreamNodeIds.has(current)) {
      continue;
    }
    downstreamNodeIds.add(current);
    for (const edge of outgoing.get(current) ?? []) {
      queue.push(edge.target);
    }
  }

  // Restore outputs, counts, and selections
  const oldOutputs = oldRun.suspendedState
    ? oldRun.suspendedState.outputs
    : oldRun.nodes.reduce((acc, node) => {
        if (node.status === "completed" && node.output !== undefined) {
          acc[node.nodeId] = node.output;
        }
        return acc;
      }, {} as Record<string, unknown>);

  const oldExecutionCounts = oldRun.suspendedState
    ? oldRun.suspendedState.executionCounts
    : oldRun.nodes.reduce((acc, node) => {
        acc[node.nodeId] = node.status === "completed" ? 1 : 0;
        return acc;
      }, {} as Record<string, number>);

  const oldRouteSelections = oldRun.suspendedState
    ? oldRun.suspendedState.routeSelections
    : {};

  // Delete state for downstream/reset nodes
  const newOutputs = { ...oldOutputs };
  const newExecutionCounts = { ...oldExecutionCounts };
  const newRouteSelections = { ...oldRouteSelections };

  for (const id of downstreamNodeIds) {
    delete newOutputs[id];
    delete newExecutionCounts[id];
    delete newRouteSelections[id];
  }

  // Reset statuses of downstream nodes, preserve completed upstream
  const newNodesState = workflow.nodes.map((node) => {
    if (downstreamNodeIds.has(node.id)) {
      return {
        nodeId: node.id,
        status: "idle" as const,
      };
    }
    const prev = oldRun.nodes.find((n) => n.nodeId === node.id);
    return prev || {
      nodeId: node.id,
      status: "idle" as const,
    };
  });

  const forkSuspendedState = {
    readyNodeIds: [nodeId],
    outputs: newOutputs,
    executionCounts: newExecutionCounts,
    routeSelections: newRouteSelections,
    approvedNodeIds: oldRun.suspendedState?.approvedNodeIds ?? [],
  };

  const newRunId = crypto.randomUUID();
  const forkRun: RunRecord = {
    id: newRunId,
    workflowId: workflow.id,
    workflowName: workflow.name,
    status: "running",
    input: oldRun.input,
    startedAt: new Date().toISOString(),
    nodes: newNodesState,
    suspendedState: forkSuspendedState,
  };

  createRun(forkRun);

  void executeWorkflow({
    workflow,
    agents: listAgents(),
    providers: listProviders(),
    runId: newRunId,
    input: oldRun.input,
    suspendedState: forkSuspendedState,
    previousNodes: newNodesState,
    onEvent: async (event) => {
      addRunEvent(event);
      publish(event);
    },
  })
    .then((result) => {
      updateRun({
        ...forkRun,
        status: result.status,
        output: result.output,
        completedAt: result.completedAt,
        nodes: result.nodes,
        suspendedState: result.suspendedState,
      });
    })
    .catch((error) => {
      updateRun({
        ...forkRun,
        status: "failed",
        completedAt: new Date().toISOString(),
        nodes: forkRun.nodes.map((node) => ({
          ...node,
          status: node.status === "idle" ? "failed" : node.status,
        })),
        output: error instanceof Error ? error.message : "Fork run failed",
      });
    });

  return getRun(newRunId);
}
