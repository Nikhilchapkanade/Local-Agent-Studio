import type {
  AgentProfile,
  ProviderCredential,
} from "@agent-studio/shared";

export type ChatMessage =
  | {
      role: "system" | "user";
      content: string;
    }
  | {
      role: "assistant";
      content: string | null;
      tool_calls?: Array<{
        id: string;
        type: "function";
        function: {
          name: string;
          arguments: string;
        };
      }>;
    }
  | {
      role: "tool";
      tool_call_id: string;
      name: string;
      content: string;
    };

export type ChatGenerationOptions = {
  temperature: number;
  maxTokens: number;
  signal?: AbortSignal;
  onDelta?: (delta: string) => void;
};

export type ChatGenerationResult = {
  text: string;
  toolCalls?: Array<{
    id: string;
    name: string;
    arguments: string;
  }>;
  usage?: {
    promptTokens?: number;
    completionTokens?: number;
    totalTokens?: number;
  };
};

export interface ChatModelAdapter {
  generate(
    messages: ChatMessage[],
    options: ChatGenerationOptions,
  ): Promise<ChatGenerationResult>;
}

type OpenAICompatibleChunk = {
  choices?: Array<{
    delta?: { content?: string };
  }>;
};

async function parseSseStream(
  response: Response,
  onDelta?: (delta: string) => void,
): Promise<string> {
  if (!response.body) {
    return "";
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let text = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    buffer += decoder.decode(value, { stream: true });

    const lines = buffer.split("\n");
    buffer = lines.pop() ?? "";

    for (const rawLine of lines) {
      const line = rawLine.trim();
      if (!line.startsWith("data:")) {
        continue;
      }
      const data = line.slice(5).trim();
      if (!data || data === "[DONE]") {
        continue;
      }
      const chunk = JSON.parse(data) as OpenAICompatibleChunk;
      const delta = chunk.choices?.[0]?.delta?.content ?? "";
      if (!delta) {
        continue;
      }
      text += delta;
      onDelta?.(delta);
    }
  }

  return text;
}

class OpenAICompatibleAdapter implements ChatModelAdapter {
  constructor(
    private readonly provider: ProviderCredential,
    private readonly agent: AgentProfile,
  ) {}

  private getToolsJson() {
    const tools: any[] = [];
    const allowed = this.agent.allowedTools || [];
    if (allowed.includes("http")) {
      tools.push({
        type: "function",
        function: {
          name: "http_request",
          description: "Perform an HTTP request to fetch external data or trigger an action.",
          parameters: {
            type: "object",
            properties: {
              url: { type: "string", description: "The URL to send the request to." },
              method: { type: "string", enum: ["GET", "POST"], description: "HTTP method." },
              headers: { type: "string", description: "JSON string of HTTP headers." },
              body: { type: "string", description: "The body of the POST request." }
            },
            required: ["url", "method"]
          }
        }
      });
    }
    if (allowed.includes("delegation")) {
      tools.push({
        type: "function",
        function: {
          name: "delegate_to_agent",
          description: "Delegate a task to another worker agent and get their response.",
          parameters: {
            type: "object",
            properties: {
              agentId: { type: "string", description: "The ID of the target agent profile (e.g. agent-researcher, agent-developer)." },
              prompt: { type: "string", description: "The instruction prompt for the target agent." }
            },
            required: ["agentId", "prompt"]
          }
        }
      });
    }
    return tools.length > 0 ? tools : undefined;
  }

  async generate(
    messages: ChatMessage[],
    options: ChatGenerationOptions,
  ): Promise<ChatGenerationResult> {
    const baseUrl =
      this.provider.type === "openai"
        ? "https://api.openai.com/v1"
        : this.provider.baseUrl;

    if (!baseUrl) {
      throw new Error(`Provider ${this.provider.name} is missing a base URL.`);
    }

    if (!this.provider.apiKey && this.provider.type !== "ollama") {
      throw new Error(`Provider ${this.provider.name} is missing an API key.`);
    }

    const tools = this.getToolsJson();
    const useTools = tools !== undefined;

    const response = await fetch(`${baseUrl}/chat/completions`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...this.provider.customHeaders,
        ...(this.provider.apiKey
          ? { Authorization: `Bearer ${this.provider.apiKey}` }
          : {}),
      },
      body: JSON.stringify({
        model: this.agent.model || this.provider.defaultModel,
        messages,
        temperature: options.temperature,
        max_tokens: options.maxTokens,
        stream: !useTools,
        ...(useTools ? { tools } : {}),
      }),
      signal: options.signal,
    });

    if (!response.ok) {
      throw new Error(
        `Provider call failed (${response.status}): ${await response.text()}`,
      );
    }

    if (useTools) {
      const json = (await response.json()) as {
        choices?: Array<{
          message?: {
            content?: string | null;
            tool_calls?: Array<{
              id: string;
              type: string;
              function: {
                name: string;
                arguments: string;
              };
            }>;
          };
        }>;
      };
      const choice = json.choices?.[0];
      const text = choice?.message?.content ?? "";
      const toolCalls = choice?.message?.tool_calls?.map((tc) => ({
        id: tc.id,
        name: tc.function.name,
        arguments: tc.function.arguments,
      }));
      return { text, toolCalls };
    }

    const text = await parseSseStream(response, options.onDelta);
    return { text };
  }
}

export function createChatModelAdapter(
  provider: ProviderCredential,
  agent: AgentProfile,
): ChatModelAdapter {
  if (
    provider.type === "openai" ||
    provider.type === "openai_compatible" ||
    provider.type === "ollama"
  ) {
    return new OpenAICompatibleAdapter(provider, agent);
  }

  throw new Error(`Unsupported provider type: ${provider.type satisfies never}`);
}
