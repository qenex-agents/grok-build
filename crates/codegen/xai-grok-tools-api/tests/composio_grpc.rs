//! In-process stand-in for `@composio/grpc`: list, finalize, and execute
//! on `xai.grok.tools.v1.GrokToolsService`, including the bearer check.

use std::sync::Mutex;

use tonic::metadata::MetadataValue;
use tonic::transport::Endpoint;
use tonic::{Request, Response, Status};
use xai_grok_tools_api::composio_grpc::{
    CallResult, ComposioGrpcClient, ComposioGrpcTarget, ExecuteOptions, FinalizeTool,
    ListToolsOptions,
};
use xai_grok_tools_api::pb::execute_tool_response::Result as ExecuteResult;
use xai_grok_tools_api::pb::grok_tools_service_client::GrokToolsServiceClient;
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

const TOKEN: &str = "test-token";
const SLUG: &str = "GITHUB_GET_USER";

struct Mock {
    state: Mutex<Option<Vec<String>>>,
}

fn bearer_ok<T>(request: &Request<T>) -> bool {
    let expected = format!("Bearer {TOKEN}");
    request
        .metadata()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        == Some(expected.as_str())
}

fn deny<T>() -> Result<Response<T>, Status> {
    Err(Status::unauthenticated("Bearer token required"))
}

fn unimpl<T>() -> Result<Response<T>, Status> {
    Err(Status::unimplemented("not implemented"))
}

fn known(name: &str) -> Option<&'static str> {
    let trimmed = name.trim();
    if trimmed.eq_ignore_ascii_case(SLUG)
        || trimmed.eq_ignore_ascii_case("Composio:GITHUB_GET_USER")
    {
        Some(SLUG)
    } else {
        None
    }
}

fn tool_info(include_schemas: bool, enabled: bool) -> ToolInfo {
    let schema = if include_schemas {
        r#"{"type":"object","properties":{"login":{"type":"string"}}}"#.to_owned()
    } else {
        String::new()
    };
    let output = if include_schemas {
        r#"{"type":"object"}"#.to_owned()
    } else {
        String::new()
    };
    ToolInfo {
        name: SLUG.to_owned(),
        display_name: "Get user".to_owned(),
        description: "Get the authenticated GitHub user".to_owned(),
        category: ToolCategory::External as i32,
        input_schema_json: schema,
        enabled,
        capabilities: Some(ToolCapabilities {
            supports_streaming: true,
            supports_cancellation: true,
            supports_timeout: true,
            has_side_effects: true,
            is_read_only: Some(false),
            custom_capabilities: vec!["composio".to_owned(), format!("slug:{SLUG}")],
            ..ToolCapabilities::default()
        }),
        version: "current".to_owned(),
        source: ToolSource::Mcp as i32,
        output_schema_json: output,
        contract_version: "current".to_owned(),
        namespace: "Composio".to_owned(),
        ..ToolInfo::default()
    }
}

fn external_category(category: Option<i32>) -> bool {
    match category {
        None => true,
        Some(value) => {
            value == ToolCategory::Unspecified as i32 || value == ToolCategory::External as i32
        }
    }
}

fn enabled(state: &Option<Vec<String>>) -> bool {
    match state {
        None => true,
        Some(locked) => locked.iter().any(|slug| slug == SLUG),
    }
}

#[tonic::async_trait]
impl GrokToolsService for Mock {
    async fn execute_tool(
        &self,
        request: Request<ExecuteToolRequest>,
    ) -> Result<Response<ExecuteToolResponse>, Status> {
        if !bearer_ok(&request) {
            return deny();
        }
        let call_id = request
            .get_ref()
            .call_id
            .clone()
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| "call-1".to_owned());
        let name = request.get_ref().tool_name.clone();
        let state = self.state.lock().expect("lock").clone();
        let metadata = Some(ExecutionMetadata {
            duration_ms: 7,
            tool_version: "current".to_owned(),
            contract_version: "current".to_owned(),
            ..ExecutionMetadata::default()
        });
        let result = match known(&name) {
            None => ExecuteResult::Error(ToolError {
                code: ErrorCode::ToolNotFound as i32,
                message: format!("No Composio tool named \"{name}\""),
                ..ToolError::default()
            }),
            Some(_) if !enabled(&state) => ExecuteResult::Error(ToolError {
                code: ErrorCode::ToolDisabled as i32,
                message: format!("{SLUG} is not in the finalized set"),
                ..ToolError::default()
            }),
            Some(_) => ExecuteResult::Success(ToolSuccess {
                output_json: r#"{"login":"octocat"}"#.to_owned(),
                prompt_text: "login: octocat".to_owned(),
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
        if !bearer_ok(&request) {
            return deny();
        }
        let include_schemas = request.get_ref().include_schemas;
        let category = request.get_ref().category;
        if !external_category(category) {
            return Ok(Response::new(ListToolsResponse::default()));
        }
        let state = self.state.lock().expect("lock").clone();
        let info = tool_info(include_schemas, enabled(&state));
        Ok(Response::new(ListToolsResponse {
            total_count: 1,
            enabled_count: i32::from(info.enabled),
            tools: vec![info],
        }))
    }

    async fn get_tool_info(
        &self,
        request: Request<GetToolInfoRequest>,
    ) -> Result<Response<ToolInfo>, Status> {
        if !bearer_ok(&request) {
            return deny();
        }
        let name = request.get_ref().tool_name.clone();
        if known(&name).is_none() {
            return Err(Status::not_found(format!(
                "No Composio tool named \"{name}\""
            )));
        }
        let state = self.state.lock().expect("lock").clone();
        Ok(Response::new(tool_info(true, enabled(&state))))
    }

    async fn finalize_tool_config_request(
        &self,
        request: Request<FinalizeToolServerConfigRequest>,
    ) -> Result<Response<FinalizeToolServerConfigResponse>, Status> {
        if !bearer_ok(&request) {
            return deny();
        }
        let mut locked = Vec::new();
        for entry in &request.get_ref().tools {
            match known(&entry.id) {
                Some(slug) => locked.push(slug.to_owned()),
                None => {
                    return Ok(Response::new(FinalizeToolServerConfigResponse {
                        success: false,
                        message: format!("{}: Unknown Composio tool \"{}\"", entry.id, entry.id),
                        ..FinalizeToolServerConfigResponse::default()
                    }));
                }
            }
        }
        *self.state.lock().expect("lock") = Some(locked);
        let info = tool_info(true, true);
        Ok(Response::new(FinalizeToolServerConfigResponse {
            success: true,
            message: "ok".to_owned(),
            tools: vec![info],
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

async fn start() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let service = GrokToolsServiceServer::new(Mock {
        state: Mutex::new(None),
    });
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await
            .expect("server");
    });
    format!("http://{addr}")
}

async fn connect(address: &str) -> ComposioGrpcClient {
    ComposioGrpcClient::connect(&ComposioGrpcTarget {
        address: address.to_owned(),
        auth_token: Some(TOKEN.to_owned()),
    })
    .await
    .expect("connect")
}

#[tokio::test]
async fn lists_finalizes_and_executes_like_composio_grpc() {
    let address = start().await;
    let mut client = connect(&address).await;

    let hidden = client
        .list_tools(ListToolsOptions {
            include_schemas: false,
            include_disabled: false,
        })
        .await
        .expect("list without schemas");
    let first = hidden.first().expect("one tool");
    assert_eq!(first.slug, SLUG);
    assert_eq!(first.id, "Composio:GITHUB_GET_USER");
    assert!(first.input_schema.is_none());

    let listed = client
        .list_tools(ListToolsOptions::default())
        .await
        .expect("list");
    let tool = listed.first().expect("one tool");
    let schema_type = tool
        .input_schema
        .as_ref()
        .and_then(|schema| schema.get("type"))
        .and_then(|value| value.as_str());
    assert_eq!(schema_type, Some("object"));

    let channel = Endpoint::from_shared(address.clone())
        .expect("uri")
        .connect()
        .await
        .expect("raw channel");
    let mut raw = GrokToolsServiceClient::new(channel);
    let mut file_request = Request::new(ListToolsRequest {
        category: Some(ToolCategory::File as i32),
        include_disabled: false,
        include_schemas: true,
    });
    file_request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {TOKEN}")).expect("metadata"),
    );
    let file_tools = raw
        .list_tools(file_request)
        .await
        .expect("file category")
        .into_inner();
    assert!(file_tools.tools.is_empty());

    let fetched = client.get_tool(SLUG).await.expect("get");
    assert_eq!(fetched.id, tool.id);

    let rejected = client
        .finalize(&[FinalizeTool {
            id: "Composio:NO_SUCH_TOOL".to_owned(),
            name_override: None,
            description_override: None,
        }])
        .await
        .expect("finalize rpc");
    assert!(!rejected.success);
    assert!(rejected.tools.is_empty());

    let still = client
        .execute(SLUG, &serde_json::json!({}), ExecuteOptions::default())
        .await
        .expect("execute before a successful finalize");
    match still {
        CallResult::Ok {
            data, prompt_text, ..
        } => {
            assert_eq!(data.get("login").and_then(|v| v.as_str()), Some("octocat"));
            assert_eq!(prompt_text, "login: octocat");
        }
        CallResult::Err { message, .. } => panic!("expected success, got {message}"),
    }

    let locked = client
        .finalize(&[FinalizeTool {
            id: tool.id.clone(),
            name_override: None,
            description_override: None,
        }])
        .await
        .expect("finalize");
    assert!(locked.success);
    assert_eq!(locked.message, "ok");
    assert_eq!(
        locked.tools.first().map(|tool| tool.slug.as_str()),
        Some(SLUG)
    );

    let kept = client
        .finalize(&[FinalizeTool {
            id: "Composio:NO_SUCH_TOOL".to_owned(),
            name_override: None,
            description_override: None,
        }])
        .await
        .expect("bad finalize after lock");
    assert!(!kept.success);
    let still_locked = client
        .execute(SLUG, &serde_json::json!({}), ExecuteOptions::default())
        .await
        .expect("execute after rejected finalize");
    assert!(matches!(still_locked, CallResult::Ok { .. }));

    let missing = client
        .execute(
            "NO_SUCH_TOOL",
            &serde_json::json!({}),
            ExecuteOptions {
                call_id: Some("c-9".to_owned()),
                timeout_ms: Some(1000),
            },
        )
        .await
        .expect("execute missing");
    match missing {
        CallResult::Err { code, call_id, .. } => {
            assert_eq!(code, "ERROR_CODE_TOOL_NOT_FOUND");
            assert_eq!(call_id, "c-9");
        }
        CallResult::Ok { .. } => panic!("expected a missing-tool error"),
    }
}

fn grok_binary() -> Option<std::path::PathBuf> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../target/debug/xai-grok-pager");
    path.is_file().then_some(path)
}

fn run_grok(binary: &std::path::Path, args: &[&str], host: Option<&str>) -> std::process::Output {
    let mut command = std::process::Command::new(binary);
    command.arg("composio");
    if let Some(host) = host {
        command.args(["--address", host]);
    }
    command
        .args(args)
        .env(xai_grok_tools_api::composio_grpc::TOKEN_ENV, TOKEN)
        .env("GROK_HOME", "/tmp/grok-home-composio")
        .output()
        .expect("spawn grok")
}

#[tokio::test]
async fn grok_composio_cli_lists_finalizes_and_executes() {
    let Some(binary) = grok_binary() else {
        eprintln!("skip grok composio cli: target/debug/xai-grok-pager is not built");
        return;
    };
    let address = start().await;
    let _ready = connect(&address).await;
    let host = address
        .strip_prefix("http://")
        .expect("http address")
        .to_owned();
    let binary_for_tasks = binary.clone();
    let host_for_tasks = host.clone();

    // spawn_blocking keeps the test runtime polling the gRPC server while the CLI runs.
    let (bad_input, list, finalized, executed) = tokio::task::spawn_blocking(move || {
        let bad_input = run_grok(
            &binary_for_tasks,
            &["execute", SLUG, "--input", "[]", "--address", "127.0.0.1:1"],
            None,
        );
        let list = run_grok(&binary_for_tasks, &["list"], Some(&host_for_tasks));
        let finalized = run_grok(
            &binary_for_tasks,
            &["finalize", "Composio:GITHUB_GET_USER"],
            Some(&host_for_tasks),
        );
        let executed = run_grok(
            &binary_for_tasks,
            &["execute", SLUG, "--input", r#"{"login":"octocat"}"#],
            Some(&host_for_tasks),
        );
        (bad_input, list, finalized, executed)
    })
    .await
    .expect("cli tasks");

    assert!(
        !bad_input.status.success(),
        "bad JSON should fail before dialing"
    );
    let bad_err = String::from_utf8_lossy(&bad_input.stderr);
    assert!(bad_err.contains("JSON object"), "stderr was {bad_err}");

    let list_out = String::from_utf8_lossy(&list.stdout);
    let list_err = String::from_utf8_lossy(&list.stderr);
    assert!(list.status.success(), "list failed: {list_err}\n{list_out}");
    assert!(list_out.contains("GITHUB_GET_USER"), "{list_out}");
    assert!(list_out.contains("Composio:GITHUB_GET_USER"), "{list_out}");

    let finalize_out = String::from_utf8_lossy(&finalized.stdout);
    assert!(
        finalized.status.success(),
        "finalize failed: {}\n{finalize_out}",
        String::from_utf8_lossy(&finalized.stderr)
    );
    assert!(finalize_out.contains("ok"), "{finalize_out}");

    let execute_out = String::from_utf8_lossy(&executed.stdout);
    assert!(
        executed.status.success(),
        "execute failed: {}\n{execute_out}",
        String::from_utf8_lossy(&executed.stderr)
    );
    assert!(execute_out.contains("login: octocat"), "{execute_out}");
}

#[tokio::test]
async fn missing_bearer_is_unauthenticated() {
    let address = start().await;
    let mut client = ComposioGrpcClient::connect(&ComposioGrpcTarget {
        address,
        auth_token: None,
    })
    .await
    .expect("connect");
    let err = client
        .list_tools(ListToolsOptions::default())
        .await
        .expect_err("auth");
    let message = err.to_string();
    assert!(message.contains("Bearer token required"), "{message}");
}
