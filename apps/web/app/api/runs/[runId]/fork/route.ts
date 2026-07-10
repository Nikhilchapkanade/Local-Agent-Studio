import { NextResponse } from "next/server";
import { forkWorkflowRun } from "@/lib/runtime";
import { z } from "zod";

const forkRequestSchema = z.object({
  nodeId: z.string(),
});

export async function POST(
  request: Request,
  context: { params: Promise<{ runId: string }> },
) {
  const { runId } = await context.params;
  try {
    const payload = forkRequestSchema.parse(await request.json());
    const run = await forkWorkflowRun(runId, payload.nodeId);
    return NextResponse.json(run);
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Failed to fork run" },
      { status: 500 },
    );
  }
}
