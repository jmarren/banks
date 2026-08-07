use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    User,
    Assistant,
}

/// One piece of a message. Modeled after Claude's content blocks since
/// that's the richest shape we currently implement — a narrower provider
/// can simply never emit some variants.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Content {
    Text {
        text: String,
    },
    ToolUse {
        id: String,
        name: String,
        input: serde_json::Value,
    },
    ToolResult {
        tool_use_id: String,
        content: String,
        is_error: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub role: Role,
    pub content: Vec<Content>,
}

impl Message {
    // fn new() -> Message {
    //     Self {
    //         role: Role::User,
    //         content: vec![],
    //     }
    // }

    pub fn assistant(text: impl Into<String>) -> Self {
        Self {
            role: Role::Assistant,
            content: vec![Content::Text { text: text.into() }],
        }
    }

    pub fn user(text: impl Into<String>) -> Self {
        Self {
            role: Role::User,
            content: vec![Content::Text { text: text.into() }],
        }
    }

    // pub fn role(mut self, role: Role) -> Self {
    //     self.role = role;
    //     self
    // }
    //
    // pub fn content(mut self, text: impl Into<String>) -> Self {
    //     self.content = vec![Content::Text { text: text.into() }];
    //     self
    // }
}

/// A tool declaration. Nothing implements tools yet, but keeping this on
/// the request shape now avoids reshaping every provider impl later.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct AgentRequest {
    pub system: Option<String>,
    pub messages: Vec<Message>,
    pub tools: Vec<ToolSpec>,
    pub max_tokens: u32,
    pub temperature: Option<f32>,
}

/// Default `max_tokens` for a request when the caller doesn't set one.
pub const DEFAULT_MAX_TOKENS: u32 = 4096;

impl AgentRequest {
    pub fn new(messages: Vec<Message>) -> Self {
        Self {
            system: None,
            messages,
            tools: Vec::new(),
            max_tokens: DEFAULT_MAX_TOKENS,
            temperature: None,
        }
    }

    pub fn max_tokens(mut self, max_tokens: u32) -> Self {
        self.max_tokens = max_tokens;
        self
    }

    pub fn system(mut self, system: Option<String>) -> Self {
        self.system = system;
        self
    }

    pub fn temperature(mut self, temperature: f32) -> Self {
        self.temperature = Some(temperature);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentResponse {
    pub message: Message,
    pub stop_reason: StopReason,
    pub usage: Usage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StopReason {
    EndTurn,
    ToolUse,
    MaxTokens,
    StopSequence,
    Other(String),
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: u32,
    pub output_tokens: u32,
}

/// Incremental events for streaming consumers (the CLI prints on TextDelta).
#[derive(Debug, Clone)]
pub enum StreamEvent {
    TextDelta(String),
    ToolUseStart { id: String, name: String },
    ToolInputDelta { id: String, partial_json: String },
    MessageDone(AgentResponse),
}

#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("api error ({status}): {message}")]
    Api {
        status: reqwest::StatusCode,
        message: String,
    },
    #[error("failed to parse provider response: {0}")]
    Decode(String),
}
