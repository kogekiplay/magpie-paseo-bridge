import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, readdir, rm } from "node:fs/promises";
import { createServer } from "node:http";
import { connect, createServer as createTCPServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { gunzipSync, zstdDecompressSync } from "node:zlib";

const run = promisify(execFile);
const hook = fileURLToPath(new URL("../paseo-bridge.hooks.mjs", import.meta.url));

async function listen(server) {
  await new Promise((resolve, reject) => server.listen(0, "127.0.0.1", resolve).once("error", reject));
  return `127.0.0.1:${server.address().port}`;
}

async function close(server) {
  server.closeAllConnections?.();
  await new Promise((resolve) => server.close(resolve));
}

async function waitFor(check, label) {
  for (let attempt = 0; attempt < 60; attempt++) {
    const result = await check();
    if (result) return result;
    await delay(80);
  }
  assert.fail(label);
}

function message(id, text, phase = "final_answer") {
  return { id, type: "message", status: "completed", phase, role: "assistant", content: [{ type: "output_text", text, annotations: [] }] };
}

function sendResponses(response, id, output, stream) {
  const usage = { input_tokens: 10, output_tokens: 7, total_tokens: 17 };
  const base = { object: "response", id, model: "fixture-wire-v2", error: null, created_at: 1795090000 };
  const final = { ...base, output, status: "completed", usage };
  if (!stream) {
    response.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(final));
    return;
  }
  response.writeHead(200, { "content-type": "text/event-stream" });
  let sequence = 0;
  function event(type, data) {
    response.write(`event: ${type}\ndata: ${JSON.stringify({ type, sequence_number: sequence++, ...data })}\n\n`);
  }
  event("response.created", { response: { ...base, status: "in_progress", output: [] } });
  for (const [output_index, item] of output.entries()) {
    const partial = item.type === "message" ? { ...item, status: "in_progress", content: [] } : { ...item, status: "in_progress", arguments: "" };
    event("response.output_item.added", { output_index, item: partial });
    if (item.type === "message") {
      const [part] = item.content;
      const positions = { output_index, item_id: item.id, content_index: 0 };
      event("response.content_part.added", { ...positions, part: { ...part, text: "" } });
      event("response.output_text.delta", { ...positions, delta: part.text });
      event("response.output_text.done", { ...positions, text: part.text });
      event("response.content_part.done", { ...positions, part });
    } else {
      event("response.function_call_arguments.delta", { output_index, item_id: item.id, delta: item.arguments });
      event("response.function_call_arguments.done", { output_index, item_id: item.id, arguments: item.arguments });
    }
    event("response.output_item.done", { output_index, item });
  }
  event("response.completed", { response: final });
  response.end();
}

test("local Magpie installs the hook and indexes actual Codex sessions", { timeout: 80_000 }, async (t) => {
  const home = await mkdtemp(join(tmpdir(), "paseo-bridge-gateway-"));
  const config = join(home, "config");
  const cache = join(home, "cache");
  const codexHome = join(home, "codex");
  const index = join(config, "magpie", "paseo-bridge", "v1");
  await Promise.all([config, cache, codexHome].map((path) => mkdir(path, { mode: 0o700 })));
  const reservation = createServer();
  const gateway = await listen(reservation);
  await close(reservation);
  let websocketHandshakes = 0;
  const proxyConnections = new Set();
  const proxy = createTCPServer((client) => {
    const peer = connect(Number(gateway.split(":")[1]), "127.0.0.1");
    proxyConnections.add(client).add(peer);
    for (const [socket, other] of [[client, peer], [peer, client]]) {
      socket.on("error", () => other.destroy());
      socket.on("close", () => { proxyConnections.delete(socket); other.destroy(); });
    }
    peer.once("data", (bytes) => {
      if (bytes.subarray(0, 12).toString() === "HTTP/1.1 101") websocketHandshakes++;
    });
    client.pipe(peer).pipe(client);
  });
  const codexGateway = await listen(proxy);
  const upstreamRequests = [];
  const upstream = createServer(async (request, response) => {
    if (request.url.endsWith("/models")) {
      response.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify({ data: [{ id: "fixture-model" }] }));
      return;
    }
    if (request.method !== "POST") {
      response.writeHead(404).end();
      return;
    }
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    let body;
    try {
      let bytes = Buffer.concat(chunks);
      if (request.headers["content-encoding"] === "gzip") bytes = gunzipSync(bytes);
      if (request.headers["content-encoding"] === "zstd") bytes = zstdDecompressSync(bytes);
      body = JSON.parse(bytes);
    } catch {
      response.writeHead(400).end();
      return;
    }
    if (request.url.endsWith("/chat/completions")) {
      const base = { id: "chatcmpl_fixture", object: "chat.completion.chunk", model: "fixture-translated-v2", created: 1795090000 };
      const content = { ...base, choices: [{ index: 0, delta: { role: "assistant", content: "Translated fixture answer." }, finish_reason: null }] };
      const end = { ...base, choices: [{ index: 0, delta: {}, finish_reason: "stop" }], usage: { prompt_tokens: 10, completion_tokens: 7, total_tokens: 17 } };
      response.writeHead(200, { "content-type": "text/event-stream" })
        .end(`data: ${JSON.stringify(content)}\n\ndata: ${JSON.stringify(end)}\n\ndata: [DONE]\n\n`);
      return;
    }
    if (!request.url.endsWith("/responses")) {
      response.writeHead(404).end();
      return;
    }
    if (request.url.startsWith("/fail")) {
      response.writeHead(429, { "content-type": "application/json" }).end(JSON.stringify({ error: { message: "fixture upstream error", type: "rate_limit_error" } }));
      return;
    }
    const id = upstreamRequests.length + 1;
    const input = Array.isArray(body.input) ? body.input : [];
    const needsTool = JSON.stringify(input).includes("fixture-use-tool") && !input.some((item) => item.type === "function_call_output");
    const tool = body.tools?.find((item) => ["exec_command", "shell_command", "shell"].includes(item.name));
    const final = message(`msg_fixture_${id}`, "Fixture answer.");
    let output = [final];
    if (needsTool && tool) {
      output = [message(`msg_progress_${id}`, "Fixture progress.", "commentary"), {
        id: `fc_fixture_${id}`, type: "function_call", call_id: `call_fixture_${id}`, name: tool.name, status: "completed",
        arguments: JSON.stringify(tool.name === "exec_command" ? { cmd: "printf fixture-tool-complete" } : { command: "printf fixture-tool-complete" }),
      }];
    }
    upstreamRequests.push({ responseId: `resp_fixture_${id}`, messageId: output[0].id, needsTool, toolOutput: input.some((item) => item.type === "function_call_output"), tools: body.tools?.map(({ name, type }) => [name, type]) });
    sendResponses(response, `resp_fixture_${id}`, output, body.stream);
  });
  const upstreamAddr = await listen(upstream);
  const fixtureProxy = "http://127.0.0.1:9";
  const env = {
    PATH: process.env.PATH,
    HOME: home,
    USERPROFILE: home,
    XDG_CONFIG_HOME: config,
    XDG_CACHE_HOME: cache,
    XDG_DATA_HOME: join(home, "data"),
    XDG_STATE_HOME: join(home, "state"),
    CODEX_HOME: codexHome,
    MAGPIE_ADDR: gateway,
    MAGPIE_BUN: process.env.MAGPIE_BUN || "bun",
    HTTP_PROXY: fixtureProxy,
    HTTPS_PROXY: fixtureProxy,
    ALL_PROXY: fixtureProxy,
    NO_PROXY: "localhost,127.0.0.1,::1",
    TERM: "dumb",
  };
  const magpie = process.env.MAGPIE_BIN || "magpie";
  const codex = process.env.CODEX_BIN || "codex";
  const magpieRun = (args) => run(magpie, args, { env, cwd: home, timeout: 10_000 });
  let gatewayProcess;
  try {
    await magpieRun(["plugin", "add", hook]);
    assert.match((await magpieRun(["plugin", "list", "--json"])).stdout, /paseo-bridge\.hooks\.mjs/);
    await magpieRun(["provider", "add", "Fixture Native", "id=fixture-native", `responses=http://${upstreamAddr}/v1/responses`, "key=fixture-key", "models=fixture-model"]);
    await magpieRun(["provider", "add", "Fixture Fail", "id=fixture-fail", `responses=http://${upstreamAddr}/fail/v1/responses`, "key=fixture-key", "models=fixture-model"]);
    await magpieRun(["provider", "add", "Fixture Chat", "id=fixture-chat", `url=http://${upstreamAddr}/v1/chat/completions`, "key=fixture-key", "models=fixture-model"]);
    await magpieRun(["group", "add", "Fixture Group", "id=fixture-group", "models=fixture-fail/fixture-model,fixture-native/fixture-model", "routing=order"]);
    gatewayProcess = spawn(magpie, ["serve"], { env, cwd: home, stdio: ["ignore", "ignore", "ignore"] });
    await waitFor(async () => (await fetch(`http://${gateway}/api/hello`).catch(() => null))?.ok, "isolated Magpie did not start");

    async function lookup(session, id) {
      const hash = createHash("sha256").update(session).update("\0").update(id).digest("hex");
      return readFile(join(index, `${hash}.json`), "utf8").then(JSON.parse).catch((error) => {
        if (error.code !== "ENOENT") throw error;
      });
    }

    async function execCodex(prompt, model, websockets) {
      const command = run(codex, ["exec", "--json", "--ephemeral", "--skip-git-repo-check",
        "-s", "read-only", "-m", model,
        "-c", 'model_provider="fixture"', "-c", 'model_providers.fixture.name="Fixture"',
        "-c", `model_providers.fixture.base_url="http://${codexGateway}/agent/codex/v1"`,
        "-c", 'model_providers.fixture.wire_api="responses"',
        "-c", `model_providers.fixture.supports_websockets=${websockets}`,
        "-c", "model_providers.fixture.request_max_retries=0", "-c", "model_providers.fixture.stream_max_retries=0",
        "-c", "analytics.enabled=false", "-c", 'web_search="disabled"', prompt,
      ], { env, cwd: home, timeout: 20_000 });
      command.child.stdin.end();
      const { stdout } = await command;
      const events = stdout.trim().split("\n").map(JSON.parse);
      const session = events.find((event) => event.type === "thread.started")?.thread_id;
      assert.ok(session, "Codex supplied no session ID");
      assert.ok(events.some((event) => event.type === "item.completed" && event.item.type === "agent_message"));
      return session;
    }

    await t.test("Codex HTTP records the gateway fallback and served model", async () => {
      const session = await execCodex("fixture-normal", "fixture-group", false);
      assert.equal(websocketHandshakes, 0);
      const hit = upstreamRequests.at(-1);
      const route = await waitFor(() => lookup(session, hit.messageId), "Codex HTTP route did not appear");
      assert.deepEqual(route, { version: 1, provider: "fixture-native", model: "fixture-wire-v2", attempts: [
        { route: "fixture-fail/fixture-model", status: 429, failed: true },
        { route: "fixture-native/fixture-model", status: 200, failed: false },
      ] });
    });

    await t.test("Codex WebSocket tool turn records the final message only", async () => {
      const start = upstreamRequests.length;
      const session = await execCodex("fixture-use-tool", "fixture-native/fixture-model", true);
      assert.ok(websocketHandshakes > 0, "Codex did not connect over WebSocket");
      const turn = upstreamRequests.slice(start);
      assert.equal(turn.length, 2, JSON.stringify(turn));
      assert.equal(turn[0].needsTool, true);
      assert.equal(turn[1].toolOutput, true);
      await waitFor(() => lookup(session, turn[1].messageId), "Codex WebSocket route did not appear");
      assert.equal(await lookup(session, turn[0].messageId), undefined);
      assert.equal((await readdir(index)).length, 2);
    });

    await t.test("translated Responses JSON uses the client-visible IDs", async () => {
      const session = "fixture-translation-session";
      const response = await fetch(`http://${gateway}/agent/codex/v1/responses`, {
        method: "POST",
        headers: { "content-type": "application/json", "x-magpie-session": session },
        body: JSON.stringify({ model: "fixture-chat/fixture-model", input: "Translation fixture", stream: false }),
      });
      assert.equal(response.status, 200);
      const { id, output } = await response.json();
      const final = output.find((item) => item.role === "assistant");
      assert.ok(final?.id);
      const route = await waitFor(() => lookup(session, final.id), "Translated Responses route did not appear");
      assert.equal(route.provider, "fixture-chat");
      assert.equal(route.model, "fixture-translated-v2");
      const trace = await fetch(`http://${gateway}/v1/magpie/trace`).then((result) => result.json());
      assert.equal(trace.routes.filter((entry) => entry.session === session && entry.responseId === id).length, 1);
    });
  } finally {
    for (const socket of proxyConnections) socket.destroy();
    await close(proxy);
    if (gatewayProcess && gatewayProcess.exitCode === null && gatewayProcess.signalCode === null) {
      gatewayProcess.kill();
      await new Promise((resolve) => gatewayProcess.once("exit", resolve));
    }
    await close(upstream);
    await rm(home, { recursive: true, force: true });
  }
});
