//! Claude Messages API wire format, and conversions to/from the
//! provider-neutral types in `provider::types`.

use crate::provider::{AgentRequest, AgentResponse, Content, Message, Role, StopReason, Usage};
use serde::Deserialize;
use serde_json::{json, Value};

pub fn to_wire_request(model: &str, request: &AgentRequest, stream: bool) -> Value {
    let mut body = json!({
        "model": model,
        "max_tokens": request.max_tokens,
        "messages": request.messages.iter().map(wire_message).collect::<Vec<_>>(),
        "stream": stream,
    });

    let obj = body.as_object_mut().expect("body is always an object");

    if let Some(system) = &request.system {
        obj.insert("system".into(), json!(system));
    }
    if let Some(temperature) = request.temperature {
        obj.insert("temperature".into(), json!(temperature));
    }
    if !request.tools.is_empty() {
        obj.insert(
            "tools".into(),
            json!(request
                .tools
                .iter()
                .map(|t| json!({
                    "name": t.name,
                    "description": t.description,
                    "input_schema": t.input_schema,
                }))
                .collect::<Vec<_>>()),
        );
    }

    body
}

fn wire_message(message: &Message) -> Value {
    json!({
        "role": match message.role {
            Role::User => "user",
            Role::Assistant => "assistant",
        },
        "content": message.content.iter().map(wire_content).collect::<Vec<_>>(),
    })
}

fn wire_content(content: &Content) -> Value {
    match content {
        Content::Text { text } => json!({ "type": "text", "text": text }),
        Content::ToolUse { id, name, input } => json!({
            "type": "tool_use",
            "id": id,
            "name": name,
            "input": input,
        }),
        Content::ToolResult {
            tool_use_id,
            content,
            is_error,
        } => json!({
            "type": "tool_result",
            "tool_use_id": tool_use_id,
            "content": content,
            "is_error": is_error,
        }),
    }
}

// ---------- Response deserialization ----------

#[derive(Debug, Deserialize)]
pub struct MessageResponse {
    pub content: Vec<ResponseContentBlock>,
    pub stop_reason: Option<String>,
    pub usage: ResponseUsage,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponseContentBlock {
    Text {
        text: String,
    },
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
pub struct ResponseUsage {
    pub input_tokens: u32,
    pub output_tokens: u32,
}

pub fn stop_reason_from_wire(raw: Option<&str>) -> StopReason {
    match raw {
        Some("end_turn") => StopReason::EndTurn,
        Some("tool_use") => StopReason::ToolUse,
        Some("max_tokens") => StopReason::MaxTokens,
        Some("stop_sequence") => StopReason::StopSequence,
        Some(other) => StopReason::Other(other.to_string()),
        None => StopReason::Other("unknown".to_string()),
    }
}

impl From<MessageResponse> for AgentResponse {
    fn from(response: MessageResponse) -> Self {
        let content = response
            .content
            .into_iter()
            .filter_map(|block| match block {
                ResponseContentBlock::Text { text } => Some(Content::Text { text }),
                ResponseContentBlock::ToolUse { id, name, input } => {
                    Some(Content::ToolUse { id, name, input })
                }
                ResponseContentBlock::Unknown => None,
            })
            .collect();

        AgentResponse {
            message: Message {
                role: Role::Assistant,
                content,
            },
            stop_reason: stop_reason_from_wire(response.stop_reason.as_deref()),
            usage: Usage {
                input_tokens: response.usage.input_tokens,
                output_tokens: response.usage.output_tokens,
            },
        }
    }
}
