import { NextResponse } from "next/server";
import { resumeWorkflowRun } from "@/lib/runtime";
import { z } from "zod";

const resumeRequestSchema = z.object({
  approvedNodeId: z.string(),
});

export async function POST(
  request: Request,
  context: { params: Promise<{ runId: string }> },
) {
  const { runId } = await context.params;
  try {
    const payload = resumeRequestSchema.parse(await request.json());
    const run = await resumeWorkflowRun(runId, payload.approvedNodeId);
    return NextResponse.json(run);
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Failed to resume run" },
      { status: 500 },
    );
  }
}
