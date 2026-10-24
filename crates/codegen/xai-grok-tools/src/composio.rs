//! Register a Composio gRPC sidecar on a session tool bridge.
//!
//! The sidecar is `@composio/grpc`. It serves `xai.grok.tools.v1.GrokToolsService`
//! and holds the Composio API key. When `[composio_grpc]` is enabled, agent
//! build lists the tools, finalizes that set, and registers each one so the
//! model can call it. A sidecar that is down is logged and skipped. The same
//! address is not retried for 30 seconds after a failed dial.

use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use xai_grok_tools_api::composio_grpc::{
    CallResult, ComposioGrpcClient, ComposioGrpcError, DialOptions, ExecuteOptions, FinalizeTool,
    ListToolsOptions, load_target, model_tool_name,
};
use xai_tool_protocol::ToolId;

use crate::bridge::ToolBridge;
use crate::types::output::ToolOutput;
use crate::types::tool::{ToolKind, ToolNamespace};
use crate::types::tool_metadata::ToolMetadata;

const SESSION_DIAL: DialOptions = DialOptions {
    connect_timeout: Duration::from_secs(2),
    request_timeout: Duration::from_secs(120),
};

const UNAVAILABLE_FOR: Duration = Duration::from_secs(30);

static UNAVAILABLE: Mutex<Option<(String, Instant)>> = Mutex::new(None);

fn recently_unavailable(address: &str) -> bool {
    let Ok(guard) = UNAVAILABLE.lock() else {
        return false;
    };
    match guard.as_ref() {
        Some((cached, at)) => cached == address && at.elapsed() < UNAVAILABLE_FOR,
        None => false,
    }
}

fn mark_unavailable(address: &str) {
    if let Ok(mut guard) = UNAVAILABLE.lock() {
        *guard = Some((address.to_owned(), Instant::now()));
    }
}

fn clear_unavailable(address: &str) {
    if let Ok(mut guard) = UNAVAILABLE.lock()
        && guard.as_ref().is_some_and(|(cached, _)| cached == address)
    {
        *guard = None;
    }
}

/// List, finalize, and register Composio tools. Returns how many were registered.
///
/// `NotConfigured` and `Disabled` register nothing and do not log. A dial or
/// RPC failure logs a warning and leaves the bridge unchanged.
pub async fn attach(bridge: &ToolBridge, cwd: &Path) -> usize {
    let target = match load_target(cwd) {
        Ok(target) => target,
        Err(ComposioGrpcError::NotConfigured | ComposioGrpcError::Disabled) => return 0,
        Err(err) => {
            tracing::warn!(%err, "composio grpc config");
            return 0;
        }
    };
    if recently_unavailable(&target.address) {
        return 0;
    }
    let client = match ComposioGrpcClient::connect_with(&target, SESSION_DIAL).await {
        Ok(client) => {
            clear_unavailable(&target.address);
            client
        }
        Err(err) => {
            mark_unavailable(&target.address);
            tracing::warn!(%err, address = %target.address, "composio grpc unavailable");
            return 0;
        }
    };
    let listed = match client.list_tools(ListToolsOptions::default()).await {
        Ok(tools) => tools,
        Err(err) => {
            tracing::warn!(%err, "composio list failed");
            return 0;
        }
    };
    let entries: Vec<FinalizeTool> = listed
        .iter()
        .map(|tool| FinalizeTool {
            id: tool.id.clone(),
            name_override: model_tool_name(&tool.name).filter(|name| name != &tool.name),
            description_override: None,
        })
        .collect();
    let outcome = match client.finalize(&entries).await {
        Ok(outcome) if outcome.success => outcome,
        Ok(outcome) => {
            tracing::warn!(message = %outcome.message, "composio finalize rejected");
            return 0;
        }
        Err(err) => {
            tracing::warn!(%err, "composio finalize failed");
            return 0;
        }
    };

    let mut registered = 0;
    for tool in outcome.tools {
        let Some(name) = model_tool_name(&tool.name).or_else(|| model_tool_name(&tool.slug)) else {
            tracing::warn!(slug = %tool.slug, "skipping composio tool with an unusable name");
            continue;
        };
        let schema = tool
            .input_schema
            .clone()
            .unwrap_or_else(|| serde_json::json!({"type": "object", "properties": {}}));
        let handle = ComposioRegisteredTool {
            client: client.clone(),
            call_name: tool.name.clone(),
            description: tool.description.clone(),
            slug: tool.slug.clone(),
        };
        match bridge.register_mcp_tools(name.clone(), handle, Some(schema)) {
            Ok(()) => registered += 1,
            Err(err) => {
                tracing::warn!(%err, tool = %name, "composio tool was not registered");
            }
        }
    }
    if registered > 0 {
        tracing::info!(registered, address = %target.address, "composio tools registered");
    }
    registered
}

struct ComposioRegisteredTool {
    client: ComposioGrpcClient,
    call_name: String,
    description: String,
    slug: String,
}

impl std::fmt::Debug for ComposioRegisteredTool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComposioRegisteredTool")
            .field("call_name", &self.call_name)
            .field("slug", &self.slug)
            .finish()
    }
}

impl ToolMetadata for ComposioRegisteredTool {
    fn kind(&self) -> ToolKind {
        ToolKind::Other
    }

    fn tool_namespace(&self) -> ToolNamespace {
        ToolNamespace::MCP
    }

    fn description_template(&self) -> &str {
        &self.description
    }
}

impl xai_tool_runtime::Tool for ComposioRegisteredTool {
    type Args = serde_json::Value;
    type Output = ToolOutput;

    fn id(&self) -> ToolId {
        let qualified = format!("Composio:{}", self.slug);
        ToolId::new(qualified)
            .unwrap_or_else(|_| ToolId::new("Composio:tool").expect("static composio tool id"))
    }

    fn description(
        &self,
        _ctx: &xai_tool_runtime::ListToolsContext,
    ) -> xai_tool_types::ToolDescription {
        xai_tool_types::ToolDescription::new(&self.call_name, &self.description)
    }

    async fn run(
        &self,
        _ctx: xai_tool_runtime::ToolCallContext,
        args: serde_json::Value,
    ) -> Result<ToolOutput, xai_tool_runtime::ToolError> {
        let input = if args.is_null() {
            serde_json::json!({})
        } else if args.is_object() {
            args
        } else {
            return Err(xai_tool_runtime::ToolError::invalid_arguments(
                "Composio tool arguments must be a JSON object",
            ));
        };
        match self
            .client
            .execute(&self.call_name, &input, ExecuteOptions::default())
            .await
        {
            Ok(CallResult::Ok {
                prompt_text, data, ..
            }) => {
                let text = if prompt_text.is_empty() {
                    serde_json::to_string_pretty(&data).unwrap_or_else(|_| "{}".to_owned())
                } else {
                    prompt_text
                };
                Ok(ToolOutput::Text(text.into()))
            }
            Ok(CallResult::Err { code, message, .. }) => Err(xai_tool_runtime::ToolError::custom(
                "composio",
                format!("{code}: {message}"),
            )),
            Err(err) => Err(xai_tool_runtime::ToolError::custom(
                "composio",
                err.to_string(),
            )),
        }
    }
}
