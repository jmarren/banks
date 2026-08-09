mod stream;
mod wire;

use super::{AgentRequest, AgentResponse, Provider, ProviderError, StreamEvent};
use async_trait::async_trait;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;
use reqwest::Client;

const API_BASE: &str = "https://api.anthropic.com/v1/messages";
const ANTHROPIC_VERSION: &str = "2023-06-01";

pub struct ClaudeProvider {
    client: Client,
    api_key: String,
    model: String,
}

impl ClaudeProvider {
    pub fn new(api_key: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            api_key: api_key.into(),
            model: model.into(),
        }
    }

    fn request_builder(&self, body: &serde_json::Value) -> reqwest::RequestBuilder {
        self.client
            .post(API_BASE)
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", ANTHROPIC_VERSION)
            .header("content-type", "application/json")
            .json(body)
    }
}

async fn error_from_response(response: reqwest::Response) -> ProviderError {
    let status = response.status();
    let message = response
        .text()
        .await
        .unwrap_or_else(|e| format!("<failed to read error body: {e}>"));
    ProviderError::Api { status, message }
}

#[async_trait]
impl Provider for ClaudeProvider {
    async fn send(&self, request: AgentRequest) -> Result<AgentResponse, ProviderError> {
        let body = wire::to_wire_request(&self.model, &request, false);
        let response = self.request_builder(&body).send().await?;

        if !response.status().is_success() {
            return Err(error_from_response(response).await);
        }

        let wire_response: wire::MessageResponse = response
            .json()
            .await
            .map_err(|e| ProviderError::Decode(e.to_string()))?;

        Ok(wire_response.into())
    }

    async fn stream(
        &self,
        request: AgentRequest,
    ) -> Result<BoxStream<'static, Result<StreamEvent, ProviderError>>, ProviderError> {
        let body = wire::to_wire_request(&self.model, &request, true);
        let response = self.request_builder(&body).send().await?;

        if !response.status().is_success() {
            return Err(error_from_response(response).await);
        }

        let byte_stream = response.bytes_stream();
        Ok(stream::full_stream_transform(byte_stream).boxed())
        // Ok(stream::parse_sse(byte_stream).boxed())
    }
}
