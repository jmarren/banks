//! Parses Claude's SSE stream into `StreamEvent`s, accumulating enough
//! state to emit a final `AgentResponse` on `message_stop`.

use crate::provider::{AgentResponse, Content, Message, ProviderError, Role, StreamEvent, Usage};
use bytes::Bytes;
use eventsource_stream::Eventsource;
use futures_core::Stream;
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::Value;

use super::wire::stop_reason_from_wire;

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

#[derive(Debug, Deserialize)]
struct MessageStartPayload {
    usage: Option<UsagePayload>,
}

#[derive(Debug, Deserialize)]
struct UsagePayload {
    input_tokens: u32,
    #[serde(default)]
    output_tokens: u32,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ContentBlockStart {
    Text { text: String },
    ToolUse { id: String, name: String },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ContentDelta {
    TextDelta { text: String },
    InputJsonDelta { partial_json: String },
    #[serde(other)]
    Unknown,
}

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

    fn finish(self) -> AgentResponse {
        let content = self
            .blocks
            .into_iter()
            .map(|block| match block {
                PendingBlock::Text(text) => Content::Text { text },
                PendingBlock::ToolUse { id, name, json_buf } => {
                    let input: Value =
                        serde_json::from_str(&json_buf).unwrap_or(Value::Object(Default::default()));
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
}

pub fn parse_sse(
    byte_stream: impl Stream<Item = reqwest::Result<Bytes>> + Send + 'static,
) -> impl Stream<Item = Result<StreamEvent, ProviderError>> + Send + 'static {
    let events = byte_stream
        .map(|res| res.map_err(std::io::Error::other))
        .eventsource();

    async_stream::stream! {
        let mut acc = Accumulator::new();
        futures_util::pin_mut!(events);

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

            let raw: RawEvent = match serde_json::from_str(&event.data) {
                Ok(r) => r,
                Err(e) => {
                    yield Err(ProviderError::Decode(e.to_string()));
                    return;
                }
            };

            match raw {
                RawEvent::MessageStart { message } => {
                    if let Some(usage) = message.usage {
                        acc.input_tokens = usage.input_tokens;
                        acc.output_tokens = usage.output_tokens;
                    }
                }
                RawEvent::ContentBlockStart { index, content_block } => {
                    match content_block {
                        ContentBlockStart::Text { text } => {
                            debug_assert_eq!(index, acc.blocks.len());
                            if !text.is_empty() {
                                yield Ok(StreamEvent::TextDelta(text.clone()));
                            }
                            acc.blocks.push(PendingBlock::Text(text));
                        }
                        ContentBlockStart::ToolUse { id, name } => {
                            debug_assert_eq!(index, acc.blocks.len());
                            yield Ok(StreamEvent::ToolUseStart { id: id.clone(), name: name.clone() });
                            acc.blocks.push(PendingBlock::ToolUse {
                                id,
                                name,
                                json_buf: String::new(),
                            });
                        }
                        ContentBlockStart::Unknown => {
                            acc.blocks.push(PendingBlock::Text(String::new()));
                        }
                    }
                }
                RawEvent::ContentBlockDelta { index, delta } => {
                    let Some(block) = acc.blocks.get_mut(index) else { continue };
                    match (block, delta) {
                        (PendingBlock::Text(text), ContentDelta::TextDelta { text: chunk }) => {
                            text.push_str(&chunk);
                            yield Ok(StreamEvent::TextDelta(chunk));
                        }
                        (
                            PendingBlock::ToolUse { id, json_buf, .. },
                            ContentDelta::InputJsonDelta { partial_json },
                        ) => {
                            json_buf.push_str(&partial_json);
                            yield Ok(StreamEvent::ToolInputDelta {
                                id: id.clone(),
                                partial_json,
                            });
                        }
                        _ => {}
                    }
                }
                RawEvent::ContentBlockStop { .. } => {}
                RawEvent::MessageDelta { delta } => {
                    if let Some(reason) = delta.stop_reason {
                        acc.stop_reason = Some(reason);
                    }
                    if let Some(usage) = delta.usage {
                        acc.output_tokens = usage.output_tokens;
                    }
                }
                RawEvent::MessageStop => {
                    let response = std::mem::replace(&mut acc, Accumulator::new()).finish();
                    yield Ok(StreamEvent::MessageDone(response));
                    return;
                }
                RawEvent::Ping | RawEvent::Unknown => {}
            }
        }
    }
}
