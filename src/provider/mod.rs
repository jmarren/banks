mod types;

pub mod claude;

pub use types::*;

use async_trait::async_trait;
use futures_core::stream::BoxStream;

#[async_trait]
pub trait Provider: Send + Sync {
    async fn send(&self, request: AgentRequest) -> Result<AgentResponse, ProviderError>;

    async fn stream(
        &self,
        request: AgentRequest,
    ) -> Result<BoxStream<'static, Result<StreamEvent, ProviderError>>, ProviderError>;
}
