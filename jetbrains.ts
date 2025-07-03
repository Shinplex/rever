/**
 * JetBrains AI ⇢ OpenAI Compatible API ( single-file Deno port )
 * Part of “Rever by Shinplex”  https://github.com/Shinplex/rever
 * Licensed under the APGL v3
 *
 * -- Single-file, zero-dependency implementation (uses only Deno std/http).
 * -- Hard-coded client API keys, remote JWTs and model list as requested.
 *
 * Run: deno run --allow-net jetbrains.ts
 */

import {
  serve,
  Server,
  ServerRequest,
} from "https://deno.land/std@0.224.0/http/server.ts";

// ───────────────────────────────────────── constants ──
const CLIENT_API_KEYS = new Set<string>([
  // >>> your client keys here <<<
  "sk-your-custom-key-here",
]);

const JETBRAINS_JWTS = [
  // >>> remote JetBrains AI JWTs here <<<
  "your-jwt-here-1",
  "your-jwt-here-2",
];

const MODELS = [
  "anthropic-claude-3.7-sonnet",
  "anthropic-claude-4-sonnet",
  "google-chat-gemini-pro-2.5",
  "openai-o4-mini",
  "openai-o3-mini",
  "openai-o3",
  "openai-o1",
  "openai-gpt-4o",
  "anthropic-claude-3.5-sonnet",
  "openai-gpt4.1",
];

// ──────────────────────────────────── helpers / types ──
let jwtIndex = 0;
function nextJwt(): string {
  if (JETBRAINS_JWTS.length === 0) {
    throw new Error("JetBrains JWT list is empty");
  }
  const tok = JETBRAINS_JWTS[jwtIndex];
  jwtIndex = (jwtIndex + 1) % JETBRAINS_JWTS.length;
  return tok;
}

interface ChatMessage {
  role: "user" | "assistant" | "system" | string;
  content: string;
}
interface ChatCompletionRequest {
  model: string;
  messages: ChatMessage[];
  stream?: boolean;
  temperature?: number;
  max_tokens?: number;
  top_p?: number;
}

function unauthorized(req: ServerRequest, msg: string, status = 401) {
  return req.respond({
    status,
    headers: new Headers({
      "Content-Type": "application/json",
      "WWW-Authenticate": "Bearer",
    }),
    body: JSON.stringify({ error: msg }),
  });
}

function jsonResponse(data: unknown, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json",
    },
  });
}

// ────────────────────────────────────── SSE adapter ──
function openaiSSEStream(
  remote: ReadableStream<Uint8Array>,
  model: string,
): ReadableStream<Uint8Array> {
  const decoder = new TextDecoder();
  const encoder = new TextEncoder();
  let streamId = crypto.randomUUID();
  let firstChunk = false;

  return new ReadableStream({
    async start(controller) {
      const reader = remote.getReader();
      let buf = "";

      function emitLine(line: string) {
        controller.enqueue(encoder.encode(line));
      }

      try {
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          buf += decoder.decode(value, { stream: true });

          let idx: number;
          while ((idx = buf.indexOf("\n")) >= 0) {
            const raw = buf.slice(0, idx).trim();
            buf = buf.slice(idx + 1);

            if (!raw || raw === "data: end") continue;
            if (!raw.startsWith("data: ")) continue;

            try {
              const j = JSON.parse(raw.slice(6));
              const type = j.type;

              if (type === "Content") {
                const delta: Record<string, unknown> = firstChunk
                  ? { content: j.content }
                  : { role: "assistant", content: j.content };
                firstChunk = true;
                const payload = {
                  id: streamId,
                  object: "chat.completion.chunk",
                  created: Math.floor(Date.now() / 1000),
                  model,
                  choices: [{ delta, index: 0, finish_reason: null }],
                };
                emitLine(`data: ${JSON.stringify(payload)}\n\n`);
              } else if (type === "FinishMetadata") {
                const payload = {
                  id: streamId,
                  object: "chat.completion.chunk",
                  created: Math.floor(Date.now() / 1000),
                  model,
                  choices: [{
                    delta: {},
                    index: 0,
                    finish_reason: "stop",
                  }],
                };
                emitLine(`data: ${JSON.stringify(payload)}\n\n`);
                emitLine(`data: [DONE]\n\n`);
                controller.close();
              }
            } catch {
              // ignore parse errors
            }
          }
        }
      } catch (e) {
        const errPayload = {
          id: streamId,
          object: "chat.completion.chunk",
          created: Math.floor(Date.now() / 1000),
          model,
          choices: [{
            delta: {
              role: "assistant",
              content: `内部错误: ${e}`,
            },
            index: 0,
            finish_reason: "stop",
          }],
        };
        emitLine(`data: ${JSON.stringify(errPayload)}\n\n`);
        emitLine(`data: [DONE]\n\n`);
        controller.close();
      }
    },
  });
}

async function aggregateStreamToJSON(
  stream: ReadableStream<Uint8Array>,
  model: string,
) {
  const decoder = new TextDecoder();
  const reader = stream.getReader();
  let buf = "";
  let content = "";

  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    buf += decoder.decode(value, { stream: true });

    let idx: number;
    while ((idx = buf.indexOf("\n")) >= 0) {
      const line = buf.slice(0, idx).trim();
      buf = buf.slice(idx + 1);

      if (!line.startsWith("data: ")) continue;
      if (line === "data: [DONE]") continue;

      try {
        const j = JSON.parse(line.slice(6));
        const delta = j?.choices?.[0]?.delta;
        if (delta?.content) content += delta.content as string;
      } catch {
        /* ignore */
      }
    }
  }

  return {
    id: `chatcmpl-${crypto.randomUUID()}`,
    object: "chat.completion",
    created: Math.floor(Date.now() / 1000),
    model,
    choices: [{
      index: 0,
      message: { role: "assistant", content },
      finish_reason: "stop",
    }],
    usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
  };
}

// ───────────────────────────────────────── handlers ──
async function handleModels(req: ServerRequest) {
  const now = Math.floor(Date.now() / 1000);
  const data = MODELS.map((id) => ({
    id,
    object: "model",
    created: now,
    owned_by: "jetbrains-ai",
  }));
  return jsonResponse({ object: "list", data });
}

async function handleChatCompletions(req: ServerRequest) {
  // auth
  const auth = req.headers.get("authorization") ?? "";
  const m = auth.match(/^Bearer (.+)$/);
  if (!m) return unauthorized(req, "需要在 Authorization header 中提供 API 密钥");
  const token = m[1];
  if (!CLIENT_API_KEYS.has(token)) {
    return unauthorized(req, "无效的客户端 API 密钥", 403);
  }

  // body
  const bodyText = await req.text();
  let body: ChatCompletionRequest;
  try {
    body = JSON.parse(bodyText);
  } catch {
    return jsonResponse({ error: "请求 JSON 解析失败" }, 400);
  }

  const { model, messages, stream = false } = body;
  if (!MODELS.includes(model)) {
    return jsonResponse({ error: `模型 ${model} 未找到` }, 404);
  }

  // convert messages to remote format
  const jbMessages = messages.map((m) => ({
    type: `${m.role}_message`,
    content: m.content,
  }));

  const payload = {
    prompt: "ij.chat.request.new-chat-on-start",
    profile: model,
    chat: { messages: jbMessages },
    parameters: { data: [] },
  };

  // remote call
  const remoteResp = await fetch(
    "https://api.jetbrains.ai/user/v5/llm/chat/stream/v7",
    {
      method: "POST",
      headers: {
        "User-Agent": "ktor-client",
        Accept: "text/event-stream",
        "Content-Type": "application/json",
        "Accept-Charset": "UTF-8",
        "Cache-Control": "no-cache",
        "grazie-agent":
          '{"name":"aia:deno","version":"0.0.1:rever-single-file"}',
        "grazie-authenticate-jwt": nextJwt(),
      },
      body: JSON.stringify(payload),
    },
  );

  if (!remoteResp.ok) {
    return jsonResponse(
      { error: `上游 JetBrains AI 错误: ${remoteResp.status}` },
      502,
    );
  }

  if (stream) {
    // passthrough SSE (after adaptation)
    const adapted = openaiSSEStream(remoteResp.body!, model);
    return new Response(adapted, {
      status: 200,
      headers: {
        "Content-Type": "text/event-stream",
        Connection: "keep-alive",
        "Cache-Control": "no-cache",
      },
    });
  } else {
    const adaptedStream = openaiSSEStream(remoteResp.body!, model);
    const result = await aggregateStreamToJSON(adaptedStream, model);
    return jsonResponse(result, 200);
  }
}

// ────────────────────────────────────────── router ──
async function router(req: ServerRequest) {
  const { method, url } = req;
  if (method === "GET" && url === "/v1/models") {
    return handleModels(req);
  }
  if (method === "POST" && url === "/v1/chat/completions") {
    return handleChatCompletions(req);
  }
  return jsonResponse({ error: "未找到路由" }, 404);
}

// ──────────────────────────────────────── server ──
console.log(
  "JetBrains AI OpenAI Compatible API (Rever / Deno) 正在启动，端口 8000",
);
serve(router, { port: 8000 });
