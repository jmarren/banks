# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`banks` is a minimal Rust CLI AI agent. It runs a stdin/stdout REPL that talks to Claude via the Messages API, streaming responses token-by-token. The core design goal is a provider-agnostic `Provider` trait — only a Claude implementation exists today, but the neutral message/request/response types are shaped so a second provider (or tool execution) can be added without reworking the agent loop.

## Commands

```bash
cargo build              # build
cargo run                # run the REPL (requires ANTHROPIC_API_KEY, see below)
cargo run -- --model claude-opus-5 --system "..." --max-tokens 8192
cargo clippy --all-targets   # lint — keep this clean, it's part of how this repo is checked
cargo fmt                # format (there is currently unformatted code; run before committing changes you touch)
```

There are no `#[test]` functions in the codebase yet — `cargo test` builds but has nothing to run.

### Runtime config

- `ANTHROPIC_API_KEY` must be set (env var or `.env` file in the project root — loaded automatically via `dotenvy` at startup). Missing `.env` is fine; a malformed one is a hard error.
- `RUST_LOG` controls log verbosity (`tracing_subscriber::EnvFilter`), e.g. `RUST_LOG=debug cargo run`. Note: very little of the codebase actually emits `tracing` events yet — the subscriber is wired up but mostly unused.
- CLI flags: `--model` (default `claude-sonnet-5`), `--system`, `--max-tokens` (default `4096`).
- In the REPL, `/exit` or `/quit` ends the session.

## Architecture

### The `Provider` trait is the abstraction boundary

`src/provider/mod.rs` defines `Provider`, an async trait with `send` (non-streaming) and `stream` (streaming) methods, both taking a provider-neutral `AgentRequest` and returning provider-neutral types (`AgentResponse` / a `BoxStream` of `StreamEvent`). `Agent<P: Provider>` (`src/agent.rs`) is generic over this trait and never references `ClaudeProvider` directly — it only ever calls `send`/`stream` and works with the neutral types. Adding a second provider means writing a new `impl Provider for ...`, not touching the agent loop.

The one place this abstraction leaks is `ProviderError` (`src/provider/types.rs`), which bakes in `reqwest::Error` and `reqwest::StatusCode` directly — a deliberate tradeoff since HTTP is assumed to be the transport for the foreseeable future.

### Neutral types vs. Claude wire format

- `src/provider/types.rs` holds the provider-neutral model: `Message`, `Content` (an enum: `Text` / `ToolUse` / `ToolResult`), `Role`, `AgentRequest`, `AgentResponse`, `StopReason`, `Usage`, `StreamEvent`, `ProviderError`. `Message` and `AgentRequest` use consuming builder methods (`Message::user(...)`, `AgentRequest::new(messages).max_tokens(...).system(...)`) rather than public struct-literal construction.
- `ToolUse`, `ToolResult`, and `ToolSpec` are modeled but **not wired up anywhere** — no tool execution exists yet. They're present so adding tools later won't reshape the request/response contract. If a `StreamEvent::ToolUseStart` arrives, `Agent` currently just prints a "not yet supported" message and moves on.
- `src/provider/claude/` is the only `Provider` implementation, split by concern:
  - `mod.rs` — `ClaudeProvider` struct (holds `reqwest::Client`, API key, model name), builds HTTP requests (`x-api-key` / `anthropic-version` headers) against `https://api.anthropic.com/v1/messages`.
  - `wire.rs` — translates neutral types to/from Claude's exact JSON wire format. `to_wire_request` builds the outbound JSON by hand with `serde_json::json!`, conditionally inserting optional fields (`system`, `temperature`, `tools`) so absent fields aren't sent as `null`/empty. The inbound side (`MessageResponse` et al.) is only used by the non-streaming `send` path.
  - `stream.rs` — parses Claude's SSE stream into `StreamEvent`s using the `async_stream::stream! { ... }` macro (which is what makes `yield` usable inside `parse_sse` — not standard Rust, macro-provided). An `Accumulator` reassembles the full message across `content_block_start`/`content_block_delta`/`message_stop` events while also yielding incremental `TextDelta`/`ToolUseStart`/`ToolInputDelta` events live, so the CLI can print tokens as they arrive while still ending up with a complete `AgentResponse` on `MessageDone`.

### Agent loop

`Agent::turn` (`src/agent.rs`) is the per-input entry point: it pushes the user message onto `history`, builds an `AgentRequest` from `(system, history, max_tokens)`, calls `provider.stream(...)`, and hands the resulting event stream to `handle_events`, which dispatches each `StreamEvent` variant to its own handler method (`handle_text_delta`, `handle_tool_use_start`, `handle_tool_input_delta`, `handle_message_done`). The full conversation history is resent on every turn — there is no context trimming/summarization yet.

`main.rs` is a thin REPL shell around `Agent`: parse args, load `.env`, init tracing, read stdin lines in a blocking loop, call `agent.turn(...).await` per line.

### Diagrams

`docs/architecture.md` has a Mermaid class diagram and a sequence diagram (one full CLI turn, from user input through the SSE stream back to stdout) — useful for orienting before making structural changes to the provider/agent split.

## Known gaps (don't be surprised by these)

- No tool execution — `ToolUse`/`ToolResult` types exist but nothing invokes tools.
- No context trimming — every turn resends the entire history.
- `cargo fmt` is not currently clean on the whole tree; `cargo clippy --all-targets` is.
- `tracing` is initialized but barely used — most of the codebase has no instrumentation yet.
