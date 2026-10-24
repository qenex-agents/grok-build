//! A session tool bridge lists, finalizes, and calls a GrokToolsService sidecar.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tonic::{Request, Response, Status};
use xai_grok_tools::bridge::ToolBridge;
use xai_grok_tools::computer::local::{LocalFs, LocalTerminalBackend};
use xai_grok_tools::registry::types::{SessionContext, ToolServerConfig};
use xai_grok_tools_api::pb::execute_tool_response::Result as ExecuteResult;
use xai_grok_tools_api::pb::grok_tools_service_server::{GrokToolsService, GrokToolsServiceServer};
use xai_grok_tools_api::pb::{
    ClearToolOverrideRequest, ClearToolOverrideResponse, DisableToolRequest, DisableToolResponse,
    EnableToolRequest, EnableToolResponse, ErrorCode, ExecuteToolRequest, ExecuteToolResponse,
    ExecutionMetadata, FinalizeAgentRequest, FinalizeAgentResponse,
    FinalizeToolServerConfigRequest, FinalizeToolServerConfigResponse, GetAgentInfoRequest,
    GetAgentInfoResponse, GetCompletionStateRequest, GetCompletionStateResponse,
    GetSystemPromptRequest, GetSystemPromptResponse, GetSystemRemindersRequest,
    GetSystemRemindersResponse, GetToolInfoRequest, GetToolOptionsRequest, GetToolOptionsResponse,
    GetToolStateRequest, GetToolStateResponse, GetTruncationConfigRequest,
    GetTruncationConfigResponse, ListToolsRequest, ListToolsResponse, ResetCompletionStateRequest,
    ResetCompletionStateResponse, ResetToolOptionsRequest, ResetToolOptionsResponse,
    SetSystemRemindersRequest, SetSystemRemindersResponse, SetToolOptionsRequest,
    SetToolOptionsResponse, SetToolOverrideRequest, SetToolOverrideResponse,
    SetTruncationConfigRequest, SetTruncationConfigResponse, ToolCapabilities, ToolCategory,
    ToolError, ToolInfo, ToolSource, ToolStreamChunk, ToolSuccess,
};

struct CatalogTool {
    slug: &'static str,
    name: &'static str,
    description: &'static str,
    prompt: &'static str,
}

const CATALOG: &[CatalogTool] = &[
    CatalogTool {
        slug: "GITHUB_GET_USER",
        name: "GITHUB_GET_USER",
        description: "Get the authenticated GitHub user",
        prompt: "login: octocat",
    },
    CatalogTool {
        slug: "GITHUB_LIST_REPOS",
        name: "github.get user",
        description: "List repositories",
        prompt: "repos: 1",
    },
];

#[derive(Default)]
struct Seen {
    lists: usize,
    overrides: Vec<Option<String>>,
}

struct Mock {
    seen: Arc<Mutex<Seen>>,
    reject_finalize: bool,
}

fn unimpl<T>() -> Result<Response<T>, Status> {
    Err(Status::unimplemented("not implemented"))
}

fn catalog_tool(spec: &CatalogTool, name: &str) -> ToolInfo {
    ToolInfo {
        name: name.to_owned(),
        display_name: spec.description.to_owned(),
        description: spec.description.to_owned(),
        category: ToolCategory::External as i32,
        input_schema_json: r#"{"type":"object","properties":{"login":{"type":"string"}}}"#
            .to_owned(),
        enabled: true,
        capabilities: Some(ToolCapabilities {
            supports_streaming: true,
            supports_cancellation: true,
            supports_timeout: true,
            has_side_effects: true,
            is_read_only: Some(false),
            custom_capabilities: vec!["composio".to_owned(), format!("slug:{}", spec.slug)],
            ..ToolCapabilities::default()
        }),
        version: "current".to_owned(),
        source: ToolSource::Mcp as i32,
        output_schema_json: r#"{"type":"object"}"#.to_owned(),
        contract_version: "current".to_owned(),
        namespace: "Composio".to_owned(),
        ..ToolInfo::default()
    }
}

fn find_spec(name: &str) -> Option<&'static CatalogTool> {
    CATALOG.iter().find(|spec| {
        name == spec.slug
            || name == spec.name
            || name == format!("Composio:{}", spec.slug)
            || name
                == xai_grok_tools_api::composio_grpc::model_tool_name(spec.name).unwrap_or_default()
    })
}

#[tonic::async_trait]
impl GrokToolsService for Mock {
    async fn execute_tool(
        &self,
        request: Request<ExecuteToolRequest>,
    ) -> Result<Response<ExecuteToolResponse>, Status> {
        let body = request.into_inner();
        let call_id = body
            .call_id
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| "call-1".to_owned());
        let metadata = Some(ExecutionMetadata {
            duration_ms: 4,
            tool_version: "current".to_owned(),
            contract_version: "current".to_owned(),
            ..ExecutionMetadata::default()
        });
        let input: serde_json::Value =
            serde_json::from_str(&body.input_json).unwrap_or(serde_json::Value::Null);
        let result = match find_spec(&body.tool_name) {
            None => ExecuteResult::Error(ToolError {
                code: ErrorCode::ToolNotFound as i32,
                message: format!("No Composio tool named \"{}\"", body.tool_name),
                ..ToolError::default()
            }),
            Some(_) if input.get("fail").and_then(|value| value.as_bool()) == Some(true) => {
                ExecuteResult::Error(ToolError {
                    code: ErrorCode::ExecutionFailed as i32,
                    message: "upstream failed".to_owned(),
                    ..ToolError::default()
                })
            }
            Some(spec) => ExecuteResult::Success(ToolSuccess {
                output_json: r#"{"login":"octocat"}"#.to_owned(),
                prompt_text: spec.prompt.to_owned(),
            }),
        };
        Ok(Response::new(ExecuteToolResponse {
            call_id,
            metadata,
            result: Some(result),
        }))
    }

    type ExecuteToolStreamStream = tokio_stream::Once<Result<ToolStreamChunk, Status>>;

    async fn execute_tool_stream(
        &self,
        request: Request<ExecuteToolRequest>,
    ) -> Result<Response<Self::ExecuteToolStreamStream>, Status> {
        let _ = request;
        Err(Status::unimplemented("not implemented"))
    }

    async fn list_tools(
        &self,
        request: Request<ListToolsRequest>,
    ) -> Result<Response<ListToolsResponse>, Status> {
        let _ = request;
        self.seen.lock().expect("seen").lists += 1;
        Ok(Response::new(ListToolsResponse {
            total_count: CATALOG.len() as i32,
            enabled_count: CATALOG.len() as i32,
            tools: CATALOG
                .iter()
                .map(|spec| catalog_tool(spec, spec.name))
                .collect(),
        }))
    }

    async fn get_tool_info(
        &self,
        request: Request<GetToolInfoRequest>,
    ) -> Result<Response<ToolInfo>, Status> {
        let name = request.into_inner().tool_name;
        match find_spec(&name) {
            Some(spec) => Ok(Response::new(catalog_tool(spec, spec.name))),
            None => Err(Status::not_found(format!(
                "No Composio tool named \"{name}\""
            ))),
        }
    }

    async fn finalize_tool_config_request(
        &self,
        request: Request<FinalizeToolServerConfigRequest>,
    ) -> Result<Response<FinalizeToolServerConfigResponse>, Status> {
        let entries = request.into_inner().tools;
        {
            let mut seen = self.seen.lock().expect("seen");
            seen.overrides = entries
                .iter()
                .map(|entry| entry.name_override.clone())
                .collect();
        }
        if self.reject_finalize {
            return Ok(Response::new(FinalizeToolServerConfigResponse {
                success: false,
                message: "rejected".to_owned(),
                ..FinalizeToolServerConfigResponse::default()
            }));
        }
        let mut tools = Vec::new();
        for entry in &entries {
            let Some(spec) = CATALOG
                .iter()
                .find(|spec| entry.id == format!("Composio:{}", spec.slug))
            else {
                return Ok(Response::new(FinalizeToolServerConfigResponse {
                    success: false,
                    message: format!("{}: unknown tool", entry.id),
                    ..FinalizeToolServerConfigResponse::default()
                }));
            };
            let name = entry.name_override.as_deref().unwrap_or(spec.name);
            tools.push(catalog_tool(spec, name));
        }
        Ok(Response::new(FinalizeToolServerConfigResponse {
            success: true,
            message: "ok".to_owned(),
            tools,
            ..FinalizeToolServerConfigResponse::default()
        }))
    }

    async fn get_tool_state(
        &self,
        request: Request<GetToolStateRequest>,
    ) -> Result<Response<GetToolStateResponse>, Status> {
        let _ = request;
        unimpl()
    }
    async fn enable_tool(
        &self,
        request: Request<EnableToolRequest>,
    ) -> Result<Response<EnableToolResponse>, Status> {
        let _ = request;
        unimpl()
    }
    async fn disable_tool(
        &self,
        request: Request<DisableToolRequest>,
    ) -> Result<Response<DisableToolResponse>, Status> {
        let _ = request;
        unimpl()
    }
    async fn set_tool_options(
        &self,
        request: Request<SetToolOptionsRequest>,
    ) -> Result<Response<SetToolOptionsResponse>, Status> {
        let _ = request;
        unimpl()
    }
    async fn get_tool_options(
        &self,
        request: Request<GetToolOptionsRequest>,
    ) -> Result<Response<GetToolOptionsResponse>, Status> {
        let _ = request;
        unimpl()
    }
    async fn reset_tool_options(
        &self,
        request: Request<ResetToolOptionsRequest>,
    ) -> Result<Response<ResetToolOptionsResponse>, Status> {
        let _ = request;
        unimpl()
    }
    async fn set_tool_override(
        &self,
        request: Request<SetToolOverrideRequest>,
    ) -> Result<Response<SetToolOverrideResponse>, Status> {
        let _ = request;
        unimpl()
    }
    async fn clear_tool_override(
        &self,
        request: Request<ClearToolOverrideRequest>,
    ) -> Result<Response<ClearToolOverrideResponse>, Status> {
        let _ = request;
        unimpl()
    }
    async fn set_system_reminders(
        &self,
        request: Request<SetSystemRemindersRequest>,
    ) -> Result<Response<SetSystemRemindersResponse>, Status> {
        let _ = request;
        unimpl()
    }
    async fn get_system_reminders(
        &self,
        request: Request<GetSystemRemindersRequest>,
    ) -> Result<Response<GetSystemRemindersResponse>, Status> {
        let _ = request;
        unimpl()
    }
    async fn set_truncation_config(
        &self,
        request: Request<SetTruncationConfigRequest>,
    ) -> Result<Response<SetTruncationConfigResponse>, Status> {
        let _ = request;
        unimpl()
    }
    async fn get_truncation_config(
        &self,
        request: Request<GetTruncationConfigRequest>,
    ) -> Result<Response<GetTruncationConfigResponse>, Status> {
        let _ = request;
        unimpl()
    }
    async fn get_system_prompt(
        &self,
        request: Request<GetSystemPromptRequest>,
    ) -> Result<Response<GetSystemPromptResponse>, Status> {
        let _ = request;
        unimpl()
    }
    async fn get_agent_info(
        &self,
        request: Request<GetAgentInfoRequest>,
    ) -> Result<Response<GetAgentInfoResponse>, Status> {
        let _ = request;
        unimpl()
    }
    async fn get_completion_state(
        &self,
        request: Request<GetCompletionStateRequest>,
    ) -> Result<Response<GetCompletionStateResponse>, Status> {
        let _ = request;
        unimpl()
    }
    async fn reset_completion_state(
        &self,
        request: Request<ResetCompletionStateRequest>,
    ) -> Result<Response<ResetCompletionStateResponse>, Status> {
        let _ = request;
        unimpl()
    }
    async fn finalize_agent(
        &self,
        request: Request<FinalizeAgentRequest>,
    ) -> Result<Response<FinalizeAgentResponse>, Status> {
        let _ = request;
        unimpl()
    }
}

async fn start(reject_finalize: bool) -> (String, Arc<Mutex<Seen>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let seen = Arc::new(Mutex::new(Seen::default()));
    let service = GrokToolsServiceServer::new(Mock {
        seen: Arc::clone(&seen),
        reject_finalize,
    });
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await;
    });
    (format!("{addr}"), seen)
}

fn write_config(dir: &Path, body: &str) {
    let grok = dir.join(".grok");
    std::fs::create_dir_all(&grok).expect("mkdir");
    std::fs::write(grok.join("config.toml"), body).expect("config");
}

async fn empty_bridge(dir: &Path) -> ToolBridge {
    let ctx = SessionContext {
        backend: Arc::new(LocalTerminalBackend::new()),
        fs: Arc::new(LocalFs),
        cwd: dir.to_path_buf(),
        session_folder: dir.join("session"),
        session_env: Arc::new(std::collections::HashMap::new()),
        notification_handle: xai_grok_tools::notification::ToolNotificationHandle::noop(),
        owner_session_id: None,
        subagent: None,
        parent_scheduler_handle: None,
        skills: vec![],
        state_path: dir.join("state.json"),
        memory_backend: None,
        web_search_config: xai_grok_tools::implementations::web_search::WebSearchConfig::default(),
        web_fetch_config:
            xai_grok_tools::implementations::grok_build::web_fetch::WebFetchConfig::default(),
        lsp: None,
        image_gen_config:
            xai_grok_tools::implementations::grok_build::image_gen::ImageGenConfig::default(),
        video_gen_config:
            xai_grok_tools::implementations::grok_build::video_gen::VideoGenConfig::default(),
        app_builder_deployer_config: xai_grok_tools::implementations::grok_build::app_builder::AppBuilderDeployerConfig::default(),
        api_key_provider: None,
        auth_provider: None,
        attribution_callback: None,
        system_reminder_tag: xai_grok_tools::reminders::DEFAULT_REMINDER_TAG,
    };
    ToolBridge::finalize_builder(ToolBridge::get_builder(), ToolServerConfig::default(), ctx)
        .await
        .expect("empty tool bridge")
}

#[tokio::test]
async fn session_registers_finalized_tools_and_executes_them() {
    let (address, seen) = start(false).await;
    let dir = tempfile::tempdir().expect("temp");
    assert!(
        !dir.path().starts_with("/workspace"),
        "temp dir must not inherit the repo composio config"
    );
    write_config(
        dir.path(),
        &format!("[composio_grpc]\naddress = \"{address}\"\nenabled = true\n"),
    );
    let bridge = empty_bridge(dir.path()).await;
    let registered = xai_grok_tools::composio::attach(&bridge, dir.path()).await;
    assert_eq!(registered, 2);

    let overrides = seen.lock().expect("seen").overrides.clone();
    assert_eq!(overrides, vec![None, Some("github_get_user".to_owned())]);

    let definitions = bridge.tool_definitions().await;
    let user = definitions
        .iter()
        .find(|definition| definition.function.name == "GITHUB_GET_USER")
        .expect("user tool");
    assert_eq!(
        user.function.description.as_deref(),
        Some("Get the authenticated GitHub user")
    );
    assert_eq!(
        user.function
            .parameters
            .get("properties")
            .and_then(|properties| properties.get("login"))
            .and_then(|login| login.get("type"))
            .and_then(|kind| kind.as_str()),
        Some("string")
    );
    assert!(
        definitions
            .iter()
            .any(|definition| definition.function.name == "github_get_user")
    );

    let called = bridge
        .call(
            "GITHUB_GET_USER",
            serde_json::json!({"login": "octocat"}),
            "call-user",
        )
        .await
        .expect("execute user");
    assert!(
        called.prompt_text.contains("login: octocat"),
        "{}",
        called.prompt_text
    );

    let listed = bridge
        .call("github_get_user", serde_json::json!({}), "call-repos")
        .await
        .expect("execute renamed tool");
    assert!(
        listed.prompt_text.contains("repos: 1"),
        "{}",
        listed.prompt_text
    );

    let null_args = bridge
        .call("GITHUB_GET_USER", serde_json::Value::Null, "call-null")
        .await
        .expect("null args become an object");
    assert!(null_args.prompt_text.contains("login: octocat"));

    let bad_args = bridge
        .call("GITHUB_GET_USER", serde_json::json!([1]), "call-bad")
        .await
        .expect_err("array args");
    assert!(bad_args.to_string().contains("JSON object"), "{bad_args}");

    let failed = bridge
        .call(
            "GITHUB_GET_USER",
            serde_json::json!({"fail": true}),
            "call-fail",
        )
        .await
        .expect_err("upstream error");
    assert!(
        failed.to_string().contains("ERROR_CODE_EXECUTION_FAILED"),
        "{failed}"
    );
}

#[tokio::test]
async fn disabled_config_does_not_dial_and_a_rejected_finalize_registers_nothing() {
    let (address, seen) = start(false).await;
    let dir = tempfile::tempdir().expect("temp");
    write_config(
        dir.path(),
        &format!("[composio_grpc]\naddress = \"{address}\"\nenabled = false\n"),
    );
    let bridge = empty_bridge(dir.path()).await;
    assert_eq!(
        xai_grok_tools::composio::attach(&bridge, dir.path()).await,
        0
    );
    assert_eq!(seen.lock().expect("seen").lists, 0);

    let missing = tempfile::tempdir().expect("temp");
    let bridge = empty_bridge(missing.path()).await;
    assert_eq!(
        xai_grok_tools::composio::attach(&bridge, missing.path()).await,
        0
    );

    let (address, seen) = start(true).await;
    let dir = tempfile::tempdir().expect("temp");
    write_config(
        dir.path(),
        &format!("[composio_grpc]\naddress = \"{address}\"\nenabled = true\n"),
    );
    let bridge = empty_bridge(dir.path()).await;
    assert_eq!(
        xai_grok_tools::composio::attach(&bridge, dir.path()).await,
        0
    );
    assert!(seen.lock().expect("seen").lists >= 1);
    let err = bridge
        .call("GITHUB_GET_USER", serde_json::json!({}), "call-missing")
        .await
        .expect_err("tool was not registered");
    assert!(!err.to_string().is_empty());
}

#[tokio::test]
async fn a_down_sidecar_is_not_dialed_again_immediately() {
    let dir = tempfile::tempdir().expect("temp");
    let address = format!("192.0.2.1:{}", 20_000 + (std::process::id() % 20_000));
    write_config(
        dir.path(),
        &format!("[composio_grpc]\naddress = \"{address}\"\nenabled = true\n"),
    );
    let bridge = empty_bridge(dir.path()).await;
    let started = Instant::now();
    assert_eq!(
        xai_grok_tools::composio::attach(&bridge, dir.path()).await,
        0
    );
    let first = started.elapsed();
    let started = Instant::now();
    assert_eq!(
        xai_grok_tools::composio::attach(&bridge, dir.path()).await,
        0
    );
    let second = started.elapsed();
    assert!(
        first > Duration::from_millis(500),
        "first dial should wait for the connect timeout, took {first:?}"
    );
    assert!(
        second < Duration::from_millis(200),
        "a remembered refusal should skip the dial, took {second:?}"
    );
}
