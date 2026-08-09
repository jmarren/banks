//! Parses Claude's SSE stream into `StreamEvent`s, accumulating enough
//! state to emit a final `AgentResponse` on `message_stop`.

use crate::provider::{AgentResponse, Content, Message, ProviderError, Role, StreamEvent, Usage};
use bytes::Bytes;
use eventsource_stream::{Event, Eventsource};
use futures_core::Stream;
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::Value;

use super::wire::stop_reason_from_wire;

/// One SSE event from Claude's `text/event-stream`, tagged by its `type` field.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RawEvent {
    MessageStart {
        message: MessageStartPayload,
    },
    ContentBlockStart {
        index: usize,
        content_block: ContentBlockStart,
    },
    ContentBlockDelta {
        index: usize,
        delta: ContentDelta,
    },
    ContentBlockStop {
        index: usize,
    },
    MessageDelta {
        delta: MessageDeltaPayload,
    },
    MessageStop,
    Ping,
    #[serde(other)]
    Unknown,
}

/// Payload of a `message_start` event — carries initial token usage.
#[derive(Debug, Deserialize)]
struct MessageStartPayload {
    usage: Option<UsagePayload>,
}

/// Token counts as reported at some point in the stream; `output_tokens`
/// is typically 0 until `message_delta` near the end.
#[derive(Debug, Deserialize)]
struct UsagePayload {
    input_tokens: u32,
    #[serde(default)]
    output_tokens: u32,
}

/// The content block a `content_block_start` event is opening.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ContentBlockStart {
    Text {
        text: String,
    },
    ToolUse {
        id: String,
        name: String,
    },
    #[serde(other)]
    Unknown,
}

/// An incremental update to a block already opened by `content_block_start`.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ContentDelta {
    TextDelta {
        text: String,
    },
    InputJsonDelta {
        partial_json: String,
    },
    #[serde(other)]
    Unknown,
}

/// Payload of a `message_delta` event — final stop reason and usage update.
#[derive(Debug, Deserialize)]
struct MessageDeltaPayload {
    stop_reason: Option<String>,
    #[serde(default)]
    usage: Option<UsagePayload>,
}

/// One in-progress content block being accumulated across deltas.
enum PendingBlock {
    Text(String),
    ToolUse {
        id: String,
        name: String,
        json_buf: String,
    },
}

/// Reassembles the full message from a sequence of SSE events, indexed
/// the same way Claude indexes content blocks.
struct Accumulator {
    blocks: Vec<PendingBlock>,
    input_tokens: u32,
    output_tokens: u32,
    stop_reason: Option<String>,
}

impl Accumulator {
    fn new() -> Self {
        Self {
            blocks: Vec::new(),
            input_tokens: 0,
            output_tokens: 0,
            stop_reason: None,
        }
    }

    /// Converts accumulated blocks into a complete `AgentResponse`.
    /// A tool call whose buffered JSON fails to parse falls back to `{}`
    /// rather than erroring the whole turn.
    fn finish(self) -> AgentResponse {
        let content = self
            .blocks
            .into_iter()
            .map(|block| match block {
                PendingBlock::Text(text) => Content::Text { text },
                PendingBlock::ToolUse { id, name, json_buf } => {
                    let input: Value = serde_json::from_str(&json_buf)
                        .unwrap_or(Value::Object(Default::default()));
                    Content::ToolUse { id, name, input }
                }
            })
            .collect();

        AgentResponse {
            message: Message {
                role: Role::Assistant,
                content,
            },
            stop_reason: stop_reason_from_wire(self.stop_reason.as_deref()),
            usage: Usage {
                input_tokens: self.input_tokens,
                output_tokens: self.output_tokens,
            },
        }
    }

    /// Records initial token usage from a `message_start` event.
    fn handle_message_start(&mut self, message: MessageStartPayload) {
        if let Some(usage) = message.usage {
            self.input_tokens = usage.input_tokens;
            self.output_tokens = usage.output_tokens;
        }
    }

    /// Records the final stop reason and output token count from a
    /// `message_delta` event.
    fn handle_message_delta(&mut self, delta: MessageDeltaPayload) {
        if let Some(reason) = delta.stop_reason {
            self.stop_reason = Some(reason);
        }
        if let Some(usage) = delta.usage {
            self.output_tokens = usage.output_tokens;
        }
    }

    /// Opens a new tool-use block and returns the `ToolUseStart` event
    /// for it.
    fn handle_tool_use_start(&mut self, index: usize, id: String, name: String) -> StreamEvent {
        debug_assert_eq!(index, self.blocks.len());

        self.blocks.push(PendingBlock::ToolUse {
            id: id.clone(),
            name: name.clone(),
            json_buf: String::new(),
        });

        StreamEvent::ToolUseStart { id, name }
    }

    /// Opens a new text block, yielding a `TextDelta` immediately if the
    /// block started with non-empty text.
    fn handle_content_block_start(&mut self, index: usize, text: String) -> Option<StreamEvent> {
        debug_assert_eq!(index, self.blocks.len());

        let mut out = None;

        if !text.is_empty() {
            out = Some(StreamEvent::TextDelta(text.clone()));
        }
        self.blocks.push(PendingBlock::Text(text));

        out
    }

    /// Opens a placeholder block for a content-block type we don't
    /// recognize, so later `index`-based lookups don't go out of bounds.
    fn handle_content_block_unknown(&mut self) {
        self.blocks.push(PendingBlock::Text(String::new()));
    }

    /// Dispatches a `content_block_start` event by content-block kind.
    fn handle_content_block_start_event(
        &mut self,
        index: usize,
        content_block: ContentBlockStart,
    ) -> Option<StreamEvent> {
        match content_block {
            ContentBlockStart::Text { text } => self.handle_content_block_start(index, text),
            ContentBlockStart::ToolUse { id, name } => {
                Some(self.handle_tool_use_start(index, id, name))
            }
            ContentBlockStart::Unknown => {
                self.handle_content_block_unknown();
                None
            }
        }
    }

    /// Appends a delta to the block at `index` and yields the
    /// corresponding incremental event, if the delta kind matches the
    /// block kind at that index.
    fn handle_content_block_delta(
        &mut self,
        index: usize,
        delta: ContentDelta,
    ) -> Option<StreamEvent> {
        let block = self.blocks.get_mut(index)?;

        match (block, delta) {
            (PendingBlock::Text(text), ContentDelta::TextDelta { text: chunk }) => {
                text.push_str(&chunk);
                Some(StreamEvent::TextDelta(chunk))
            }
            (
                PendingBlock::ToolUse { id, json_buf, .. },
                ContentDelta::InputJsonDelta { partial_json },
            ) => {
                json_buf.push_str(&partial_json);
                Some(StreamEvent::ToolInputDelta {
                    id: id.clone(),
                    partial_json,
                })
            }
            _ => None,
        }
    }

    /// Dispatches a `RawEvent` other than `MessageStop` to its handler.
    /// `MessageStop` is handled by the caller, since it needs to finish
    /// and replace the accumulator rather than mutate it in place.
    fn handle_raw_event(&mut self, event: RawEvent) -> Option<StreamEvent> {
        match event {
            RawEvent::MessageStart { message } => {
                self.handle_message_start(message);
                None
            }
            RawEvent::ContentBlockStart {
                index,
                content_block,
            } => self.handle_content_block_start_event(index, content_block),
            RawEvent::ContentBlockDelta { index, delta } => {
                self.handle_content_block_delta(index, delta)
            }
            RawEvent::ContentBlockStop { .. } => None,
            RawEvent::MessageDelta { delta } => {
                self.handle_message_delta(delta);
                None
            }
            RawEvent::Ping | RawEvent::Unknown => None,
            // unreachable — the caller strips MessageStop out before delegating here
            RawEvent::MessageStop => None,
        }
    }
}

/// Composes the SSE parsing pipeline: frames the byte stream as SSE,
/// decodes each event into a `RawEvent`, then accumulates those into
/// `StreamEvent`s.
pub fn parse_sse(
    byte_stream: impl Stream<Item = reqwest::Result<Bytes>> + Send + 'static,
) -> impl Stream<Item = Result<StreamEvent, ProviderError>> + Send + 'static {
    handle_raw_events(extract_raw_events(remove_empty_and_map_errs(byte_stream)))
}

/// Frames the byte stream as SSE, mapping transport errors and dropping
/// keep-alive events with empty data. Ends the stream on the first error.
pub fn remove_empty_and_map_errs(
    byte_stream: impl Stream<Item = reqwest::Result<Bytes>> + Send + 'static,
) -> impl Stream<Item = Result<Event, ProviderError>> + Send + 'static {
    let events = byte_stream
        .map(|res| res.map_err(std::io::Error::other))
        .eventsource();

    async_stream::stream! {
        let mut events = std::pin::pin!(events);

        while let Some(event) = events.next().await {
            let event = match event {
                Ok(e) => e,
                Err(e) => {
                    yield Err(ProviderError::Decode(e.to_string()));
                    return;
                }
            };

            if event.data.is_empty() {
                continue;
            }

            yield Ok(event);
        }
    }
}

/// Decodes each SSE event's `data` field into a `RawEvent`. Ends the
/// stream on the first error, whether from upstream or from a decode
/// failure here.
fn extract_raw_events(
    event_stream: impl Stream<Item = Result<Event, ProviderError>> + Send + 'static,
) -> impl Stream<Item = Result<RawEvent, ProviderError>> + Send + 'static {
    async_stream::stream! {
        let mut event_stream = std::pin::pin!(event_stream);

        while let Some(event) = event_stream.next().await {

            // if the event is an error, yield it
            let event = match event {
                Ok(e) => e,
                Err(e) => {
                    yield Err(e);
                    return;
                }
            };

            // extract the data into a RawEvent struct and yield error if returned
            let raw: RawEvent = match serde_json::from_str(&event.data) {
                Ok(r) => r,
                Err(e) => {
                    yield Err(ProviderError::Decode(e.to_string()));
                    return;
                }
            };

            // yield RawEvent
            yield Ok(raw);
        }
    }
}

/// Accumulates `RawEvent`s into `StreamEvent`s, yielding incremental
/// events live and a final `MessageDone` on `message_stop`. Ends the
/// stream on the first error from upstream.
fn handle_raw_events(
    incoming: impl Stream<Item = Result<RawEvent, ProviderError>> + Send + 'static,
) -> impl Stream<Item = Result<StreamEvent, ProviderError>> + Send + 'static {
    async_stream::stream! {
        let mut incoming = std::pin::pin!(incoming);
        let mut acc = Accumulator::new();

        while let Some(event) = incoming.next().await {
            let event = match event {
                Ok(e) => e,
                Err(e) => {
                    yield Err(e);
                    return;
                }
            };

            match event {
                RawEvent::MessageStop => {
                    let response = std::mem::replace(&mut acc, Accumulator::new()).finish();
                    yield Ok(StreamEvent::MessageDone(response));
                    return;
                }
                _ => {
                    if let Some(result) = acc.handle_raw_event(event) {
                        yield Ok(result);
                    }
                }
            };
        }
    }
}
