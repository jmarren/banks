use crate::provider::{
    AgentRequest, AgentResponse, Content, Message, Provider, ProviderError, StreamEvent,
};
use futures_core::stream::BoxStream;
use futures_util::StreamExt;
use std::io::Write;

pub struct Agent<P: Provider> {
    provider: P,
    system: Option<String>,
    history: Vec<Message>,
    max_tokens: u32,
}

impl<P: Provider> Agent<P> {
    pub fn new(provider: P, system: Option<String>, max_tokens: u32) -> Self {
        Self {
            provider,
            system,
            history: Vec::new(),
            max_tokens,
        }
    }

    /// Pushes `input` onto history and builds an `AgentRequest` from it.
    fn request(&mut self, input: &str) -> AgentRequest {
        // create a new message from the input
        let msg = Message::user(input);
        // push it onto the history
        self.history.push(msg);
        // full history goes out each turn — no context trimming yet
        AgentRequest::new(self.history.clone())
            .system(self.system.clone())
            .max_tokens(self.max_tokens)
    }

    /// Writes a text chunk straight to stdout as it arrives.
    // stdout write/flush errors are ignored — a broken pipe here
    // shouldn't abort an in-flight model turn
    fn handle_text_delta(&self, chunk: &str) {
        let stdout = std::io::stdout();
        let mut handle = stdout.lock();
        let _ = handle.write_all(chunk.as_bytes());
        let _ = handle.flush();
    }

    /// Reports that the assistant started a tool call we can't run yet.
    fn handle_tool_use_start(&self, name: &str) {
        eprintln!("\n[tool requested: {name} — not yet supported]");
    }

    /// No-op: tool input isn't consumed until tool execution exists.
    fn handle_tool_input_delta(&self) {}

    /// Appends the completed assistant message to history and flags any
    /// tool-use content it contains as unsupported.
    fn handle_message_done(&mut self, response: AgentResponse) {
        println!();
        self.history.push(response.message);
        // re-check via history rather than `response` (moved above)
        if self
            .history
            .last()
            .map(|m| {
                m.content
                    .iter()
                    .any(|c| matches!(c, Content::ToolUse { .. }))
            })
            .unwrap_or(false)
        {
            eprintln!("[assistant requested a tool, but tool execution isn't implemented yet]");
        }
    }

    /// Drives a stream of `StreamEvent`s to completion, dispatching each
    /// to its handler.
    async fn handle_events(
        &mut self,
        mut events: BoxStream<'static, Result<StreamEvent, ProviderError>>,
    ) -> Result<(), ProviderError> {
        while let Some(event) = events.next().await {
            match event? {
                StreamEvent::TextDelta(chunk) => self.handle_text_delta(&chunk),
                StreamEvent::ToolUseStart { name, .. } => self.handle_tool_use_start(&name),
                StreamEvent::ToolInputDelta { .. } => self.handle_tool_input_delta(),
                StreamEvent::MessageDone(response) => self.handle_message_done(response),
            }
        }

        Ok(())
    }

    /// Sends `input` as a user turn, streams the assistant's reply to
    /// stdout as it arrives, and appends both turns to history.
    pub async fn turn(&mut self, input: &str) -> Result<(), ProviderError> {
        // create a new request with the input
        let req = self.request(input);

        // stream response events from the provider
        let events = self.provider.stream(req).await?;

        self.handle_events(events).await
    }
}
