//! Client for a Composio session served as `xai.grok.tools.v1.GrokToolsService`.
//!
//! `@composio/grpc` holds the Composio session and the API key. This agent
//! dials that server, lists tools, finalizes the set it will call, and
//! executes them. Tool ids are `Composio:<SLUG>`. `ListTools` asks for
//! schemas and the external category, matching `connectComposioGrpc`.
//!
//! The address comes from `[composio_grpc]` in `.grok/config.toml` or from
//! `COMPOSIO_GRPC_ADDRESS`. An optional bearer is `COMPOSIO_GRPC_TOKEN` and
//! is sent as `authorization: Bearer`. The token is never read from the
//! config file.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::Value;
use tonic::metadata::{MetadataValue, errors::InvalidMetadataValue};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

use crate::pb::grok_tools_service_client::GrokToolsServiceClient;
use crate::pb::{
    ErrorCode, ExecuteToolRequest, ExecutionOptions, FinalizeToolServerConfigRequest,
    GetToolInfoRequest, ListToolsRequest, ToolCategory, ToolConfigEntry, ToolInfo,
};

/// Namespace `@composio/grpc` puts on every tool id.
pub const COMPOSIO_TOOL_NAMESPACE: &str = "Composio";

/// Overrides `[composio_grpc] address` when set.
pub const ADDRESS_ENV: &str = "COMPOSIO_GRPC_ADDRESS";

/// Optional bearer. Sent as `authorization: Bearer <token>`.
pub const TOKEN_ENV: &str = "COMPOSIO_GRPC_TOKEN";

const CONFIG_DIR: &str = ".grok";
const CONFIG_FILE: &str = "config.toml";
const TABLE: &str = "composio_grpc";

/// Where to dial, after config and environment resolution.
#[derive(Clone, PartialEq, Eq)]
pub struct ComposioGrpcTarget {
    /// `host:port` or an `http://` / `https://` URI.
    pub address: String,
    /// Present only when the caller supplied a bearer.
    pub auth_token: Option<String>,
}

impl std::fmt::Debug for ComposioGrpcTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComposioGrpcTarget")
            .field("address", &self.address)
            .field(
                "auth_token",
                &self.auth_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// One tool returned by `ListTools` or `GetToolInfo`.
#[derive(Debug, Clone, PartialEq)]
pub struct ComposioTool {
    /// Composio slug, from the `slug:` capability or the tool name.
    pub slug: String,
    /// `Composio:<slug>`. This is the id finalize expects.
    pub id: String,
    /// Client-facing name. A finalize `name_override` replaces the slug.
    pub name: String,
    pub description: String,
    pub input_schema: Option<Value>,
    pub output_schema: Option<Value>,
    pub enabled: bool,
    pub version: String,
}

/// One entry in `FinalizeToolConfigRequest.tools`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizeTool {
    pub id: String,
    pub name_override: Option<String>,
    pub description_override: Option<String>,
}

/// Result of `ExecuteTool`, split the same way `@composio/grpc` splits it.
#[derive(Debug, Clone, PartialEq)]
pub enum CallResult {
    Ok {
        call_id: String,
        data: Value,
        prompt_text: String,
        truncated: bool,
        duration_ms: u64,
    },
    Err {
        call_id: String,
        code: String,
        message: String,
        retryable: bool,
        suggestion: Option<String>,
        details: Option<Value>,
    },
}

/// Options for [`ComposioGrpcClient::list_tools`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListToolsOptions {
    pub include_schemas: bool,
    pub include_disabled: bool,
}

impl Default for ListToolsOptions {
    fn default() -> Self {
        Self {
            include_schemas: true,
            include_disabled: false,
        }
    }
}

/// Options for [`ComposioGrpcClient::execute`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExecuteOptions {
    pub timeout_ms: Option<u64>,
    pub call_id: Option<String>,
}

#[derive(Debug)]
pub enum ComposioGrpcError {
    /// No address in config and `COMPOSIO_GRPC_ADDRESS` is unset.
    NotConfigured,
    /// `[composio_grpc] enabled = false`.
    Disabled,
    InvalidAddress(String),
    InvalidAuthToken(InvalidMetadataValue),
    Config(String),
    Transport(tonic::transport::Error),
    Status(Status),
}

impl std::fmt::Display for ComposioGrpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotConfigured => write!(
                f,
                "no Composio gRPC address; set [{TABLE}] address in {CONFIG_DIR}/{CONFIG_FILE} or {ADDRESS_ENV}"
            ),
            Self::Disabled => write!(f, "Composio gRPC is disabled in {CONFIG_DIR}/{CONFIG_FILE}"),
            Self::InvalidAddress(message) => write!(f, "invalid Composio gRPC address: {message}"),
            Self::InvalidAuthToken(_) => write!(
                f,
                "{TOKEN_ENV} is not valid to send as a gRPC authorization value"
            ),
            Self::Config(message) => write!(f, "{message}"),
            Self::Transport(err) => write!(f, "Composio gRPC connection failed: {err}"),
            Self::Status(status) => {
                write!(f, "Composio gRPC {} {}", status.code(), status.message())
            }
        }
    }
}

impl std::error::Error for ComposioGrpcError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidAuthToken(err) => Some(err),
            Self::Transport(err) => Some(err),
            Self::Status(status) => Some(status),
            _ => None,
        }
    }
}

/// `Composio:<slug>`, the id `FinalizeToolConfigRequest` expects.
pub fn composio_tool_id(slug: &str) -> String {
    format!("{COMPOSIO_TOOL_NAMESPACE}:{slug}")
}

/// Resolve the dial target from `cwd` upward and the two environment variables.
pub fn load_target(cwd: &Path) -> Result<ComposioGrpcTarget, ComposioGrpcError> {
    let address_env = std::env::var(ADDRESS_ENV).ok().filter(|v| !v.is_empty());
    let token_env = std::env::var(TOKEN_ENV).ok().filter(|v| !v.is_empty());
    resolve_target(cwd, address_env.as_deref(), token_env.as_deref())
}

/// Same as [`load_target`] with the environment values passed in.
pub fn resolve_target(
    cwd: &Path,
    address_env: Option<&str>,
    token_env: Option<&str>,
) -> Result<ComposioGrpcTarget, ComposioGrpcError> {
    let file = read_project_table(cwd)?;
    if file.as_ref().is_some_and(|found| !found.enabled) && address_env.is_none() {
        return Err(ComposioGrpcError::Disabled);
    }
    let address = address_env
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .or_else(|| file.and_then(|found| found.address))
        .ok_or(ComposioGrpcError::NotConfigured)?;
    if address.is_empty() {
        return Err(ComposioGrpcError::NotConfigured);
    }
    Ok(ComposioGrpcTarget {
        address,
        auth_token: token_env
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned),
    })
}

struct ProjectTable {
    address: Option<String>,
    enabled: bool,
}

fn read_project_table(cwd: &Path) -> Result<Option<ProjectTable>, ComposioGrpcError> {
    for dir in ancestors_inclusive(cwd) {
        let path = dir.join(CONFIG_DIR).join(CONFIG_FILE);
        if !path.is_file() {
            continue;
        }
        let text = std::fs::read_to_string(&path).map_err(|err| {
            ComposioGrpcError::Config(format!("failed to read {}: {err}", path.display()))
        })?;
        let root: toml::Value = toml::from_str(&text).map_err(|err| {
            ComposioGrpcError::Config(format!("failed to parse {}: {err}", path.display()))
        })?;
        let Some(table) = root.get(TABLE) else {
            continue;
        };
        let Some(table) = table.as_table() else {
            return Err(ComposioGrpcError::Config(format!(
                "{} in {} must be a table",
                TABLE,
                path.display()
            )));
        };
        let address = match table.get("address") {
            None => None,
            Some(toml::Value::String(value)) => {
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_owned())
                }
            }
            Some(_) => {
                return Err(ComposioGrpcError::Config(format!(
                    "{TABLE}.address in {} must be a string",
                    path.display()
                )));
            }
        };
        let enabled = match table.get("enabled") {
            None => true,
            Some(toml::Value::Boolean(value)) => *value,
            Some(_) => {
                return Err(ComposioGrpcError::Config(format!(
                    "{TABLE}.enabled in {} must be a boolean",
                    path.display()
                )));
            }
        };
        return Ok(Some(ProjectTable { address, enabled }));
    }
    Ok(None)
}

fn ancestors_inclusive(start: &Path) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    let mut cursor = Some(start);
    while let Some(dir) = cursor {
        dirs.push(dir.to_path_buf());
        cursor = dir.parent();
    }
    dirs
}

fn normalize_address(address: &str) -> Result<String, ComposioGrpcError> {
    let trimmed = address.trim();
    if trimmed.is_empty() {
        return Err(ComposioGrpcError::InvalidAddress(
            "address is empty".to_owned(),
        ));
    }
    if trimmed.contains("://") {
        Ok(trimmed.to_owned())
    } else {
        Ok(format!("http://{trimmed}"))
    }
}

type AuthedChannel = InterceptedService<Channel, AuthInterceptor>;

/// Copies the bearer onto every outgoing call.
#[derive(Clone)]
struct AuthInterceptor {
    authorization: Option<MetadataValue<tonic::metadata::Ascii>>,
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(value) = &self.authorization {
            let _ = request
                .metadata_mut()
                .insert("authorization", value.clone());
        }
        Ok(request)
    }
}

/// Dial `@composio/grpc` (or any server that speaks the same service).
pub struct ComposioGrpcClient {
    inner: GrokToolsServiceClient<AuthedChannel>,
}

impl ComposioGrpcClient {
    pub async fn connect(target: &ComposioGrpcTarget) -> Result<Self, ComposioGrpcError> {
        let uri = normalize_address(&target.address)?;
        let authorization = match &target.auth_token {
            Some(token) => Some(bearer(token)?),
            None => None,
        };
        let endpoint = Endpoint::from_shared(uri)
            .map_err(|err| ComposioGrpcError::InvalidAddress(err.to_string()))?
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30));
        let channel = endpoint
            .connect()
            .await
            .map_err(ComposioGrpcError::Transport)?;
        let inner =
            GrokToolsServiceClient::with_interceptor(channel, AuthInterceptor { authorization });
        Ok(Self { inner })
    }

    /// `include_schemas` defaults to true. Category is always external.
    pub async fn list_tools(
        &mut self,
        options: ListToolsOptions,
    ) -> Result<Vec<ComposioTool>, ComposioGrpcError> {
        let request = ListToolsRequest {
            category: Some(ToolCategory::External as i32),
            include_disabled: options.include_disabled,
            include_schemas: options.include_schemas,
        };
        let response = self
            .inner
            .list_tools(request)
            .await
            .map_err(ComposioGrpcError::Status)?
            .into_inner();
        Ok(response.tools.into_iter().map(describe_tool).collect())
    }

    pub async fn get_tool(&mut self, name: &str) -> Result<ComposioTool, ComposioGrpcError> {
        let response = self
            .inner
            .get_tool_info(GetToolInfoRequest {
                tool_name: name.to_owned(),
            })
            .await
            .map_err(ComposioGrpcError::Status)?
            .into_inner();
        Ok(describe_tool(response))
    }

    pub async fn execute(
        &mut self,
        name: &str,
        input: &Value,
        options: ExecuteOptions,
    ) -> Result<CallResult, ComposioGrpcError> {
        let request = ExecuteToolRequest {
            tool_name: name.to_owned(),
            input_json: input.to_string(),
            options: Some(ExecutionOptions {
                timeout_ms: options.timeout_ms,
                ..ExecutionOptions::default()
            }),
            call_id: options.call_id,
        };
        let response = self
            .inner
            .execute_tool(request)
            .await
            .map_err(ComposioGrpcError::Status)?
            .into_inner();
        Ok(to_call_result(response))
    }

    /// Lock the server to these ids. A bad id returns `success: false` and
    /// leaves the previous lock in place.
    pub async fn finalize(
        &mut self,
        tools: &[FinalizeTool],
    ) -> Result<FinalizeOutcome, ComposioGrpcError> {
        let request = FinalizeToolServerConfigRequest {
            tools: tools
                .iter()
                .map(|tool| ToolConfigEntry {
                    id: tool.id.clone(),
                    name_override: tool.name_override.clone(),
                    description_override: tool.description_override.clone(),
                    ..ToolConfigEntry::default()
                })
                .collect(),
            ..FinalizeToolServerConfigRequest::default()
        };
        let response = self
            .inner
            .finalize_tool_config_request(request)
            .await
            .map_err(ComposioGrpcError::Status)?
            .into_inner();
        Ok(FinalizeOutcome {
            success: response.success,
            message: response.message,
            tools: response.tools.into_iter().map(describe_tool).collect(),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FinalizeOutcome {
    pub success: bool,
    pub message: String,
    pub tools: Vec<ComposioTool>,
}

fn bearer(token: &str) -> Result<MetadataValue<tonic::metadata::Ascii>, ComposioGrpcError> {
    let value = format!("Bearer {token}");
    MetadataValue::try_from(value).map_err(ComposioGrpcError::InvalidAuthToken)
}

fn describe_tool(info: ToolInfo) -> ComposioTool {
    let slug = info
        .capabilities
        .as_ref()
        .and_then(|caps| {
            caps.custom_capabilities
                .iter()
                .find_map(|cap| cap.strip_prefix("slug:"))
        })
        .filter(|slug| !slug.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| info.name.clone());
    ComposioTool {
        id: composio_tool_id(&slug),
        name: info.name,
        description: info.description,
        input_schema: parse_schema(&info.input_schema_json),
        output_schema: parse_schema(&info.output_schema_json),
        enabled: info.enabled,
        version: info.version,
        slug,
    }
}

fn parse_schema(raw: &str) -> Option<Value> {
    if raw.is_empty() {
        return None;
    }
    serde_json::from_str(raw).ok()
}

fn to_call_result(response: crate::pb::ExecuteToolResponse) -> CallResult {
    let call_id = response.call_id;
    let duration_ms = response
        .metadata
        .as_ref()
        .map(|meta| meta.duration_ms)
        .unwrap_or(0);
    let truncated = response
        .metadata
        .as_ref()
        .is_some_and(|meta| meta.truncated);
    match response.result {
        Some(crate::pb::execute_tool_response::Result::Error(error))
            if !error.message.is_empty() =>
        {
            CallResult::Err {
                call_id,
                code: error_code_name(error.code),
                message: error.message,
                retryable: error.retryable,
                suggestion: error.suggestion.filter(|value| !value.is_empty()),
                details: error.details_json.as_deref().and_then(parse_details),
            }
        }
        Some(crate::pb::execute_tool_response::Result::Success(success)) => {
            let parsed = serde_json::from_str(&success.output_json).unwrap_or(Value::Null);
            let data = if parsed.is_object() {
                parsed
            } else {
                Value::Object(serde_json::Map::new())
            };
            CallResult::Ok {
                call_id,
                data,
                prompt_text: success.prompt_text,
                truncated,
                duration_ms,
            }
        }
        _ => CallResult::Ok {
            call_id,
            data: Value::Object(serde_json::Map::new()),
            prompt_text: String::new(),
            truncated,
            duration_ms,
        },
    }
}

fn parse_details(raw: &str) -> Option<Value> {
    if raw.is_empty() {
        return None;
    }
    Some(serde_json::from_str(raw).unwrap_or(Value::String(raw.to_owned())))
}

fn error_code_name(code: i32) -> String {
    ErrorCode::try_from(code)
        .map(|parsed| parsed.as_str_name().to_owned())
        .unwrap_or_else(|_| format!("ERROR_CODE_{code}"))
}

#[cfg(test)]
mod tests {
    use super::{ADDRESS_ENV, ComposioGrpcError, resolve_target};

    fn write_config(dir: &std::path::Path, body: &str) {
        let grok = dir.join(".grok");
        std::fs::create_dir_all(&grok).expect("mkdir");
        std::fs::write(grok.join("config.toml"), body).expect("write config");
    }

    #[test]
    fn project_config_supplies_the_address() {
        let dir = std::env::temp_dir().join(format!("composio-cfg-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        write_config(
            &dir,
            "[composio_grpc]\naddress = \"127.0.0.1:50051\"\nenabled = true\n",
        );
        let target = resolve_target(&dir, None, None).expect("target");
        assert_eq!(target.address, "127.0.0.1:50051");
        assert_eq!(target.auth_token, None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn address_env_overrides_the_file_and_token_stays_out_of_the_file() {
        let dir = std::env::temp_dir().join(format!("composio-cfg-env-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        write_config(
            &dir,
            "[composio_grpc]\naddress = \"127.0.0.1:1\"\nauth_token = \"nope\"\n",
        );
        let target = resolve_target(&dir, Some("10.0.0.8:9"), Some("sekret")).expect("target");
        assert_eq!(target.address, "10.0.0.8:9");
        assert_eq!(target.auth_token.as_deref(), Some("sekret"));
        let rendered = format!("{target:?}");
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("sekret"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn disabled_config_refuses_without_an_address_override() {
        let dir = std::env::temp_dir().join(format!("composio-cfg-off-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        write_config(
            &dir,
            "[composio_grpc]\naddress = \"127.0.0.1:1\"\nenabled = false\n",
        );
        let err = resolve_target(&dir, None, None).expect_err("disabled");
        assert!(matches!(err, ComposioGrpcError::Disabled));
        let target = resolve_target(&dir, Some("127.0.0.1:9"), None).expect("override");
        assert_eq!(target.address, "127.0.0.1:9");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_config_is_not_configured() {
        let dir = std::env::temp_dir().join(format!("composio-cfg-none-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("mkdir");
        let err = resolve_target(&dir, None, None).expect_err("missing");
        assert!(matches!(err, ComposioGrpcError::NotConfigured));
        let message = err.to_string();
        assert!(message.contains(ADDRESS_ENV));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn nearest_project_config_wins() {
        let root = std::env::temp_dir().join(format!("composio-cfg-near-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        write_config(&root, "[composio_grpc]\naddress = \"127.0.0.1:1\"\n");
        let child = root.join("nested");
        write_config(&child, "[composio_grpc]\naddress = \"127.0.0.1:2\"\n");
        let target = resolve_target(&child, None, None).expect("target");
        assert_eq!(target.address, "127.0.0.1:2");
        let _ = std::fs::remove_dir_all(&root);
    }
}
