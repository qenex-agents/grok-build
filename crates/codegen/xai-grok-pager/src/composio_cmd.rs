//! `grok composio`: call a Composio session through QENEX agents gRPC.
//!
//! The sidecar is `@composio/grpc`. It serves `xai.grok.tools.v1.GrokToolsService`
//! and holds the Composio API key. This command lists tools, finalizes the
//! set, and executes one call.

use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::Subcommand;
use xai_grok_tools_api::composio_grpc::{
    ADDRESS_ENV, CallResult, ComposioGrpcClient, ComposioTool, ExecuteOptions, FinalizeTool,
    ListToolsOptions, TOKEN_ENV, resolve_target,
};

#[derive(Debug, clap::Args, Clone)]
pub struct ComposioArgs {
    /// `host:port` or an `http://` URI. Overrides config and COMPOSIO_GRPC_ADDRESS.
    #[arg(long, global = true)]
    pub address: Option<String>,

    #[command(subcommand)]
    pub command: ComposioCommand,
}

#[derive(Debug, Subcommand, Clone)]
pub enum ComposioCommand {
    /// List Composio tools on the gRPC server
    List {
        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },
    /// Show one tool by slug, `Composio:<slug>` id, or finalized name
    Get {
        name: String,
        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },
    /// Lock the server to these tool ids. With no ids, lock every listed tool.
    Finalize {
        /// Tool id (`Composio:GITHUB_GET_USER`) or slug
        ids: Vec<String>,
        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },
    /// Execute one tool
    Execute {
        /// Slug, `Composio:<slug>` id, or finalized name
        name: String,
        /// JSON object of arguments
        #[arg(long, default_value = "{}")]
        input: String,
        /// Server-side timeout in milliseconds
        #[arg(long)]
        timeout_ms: Option<u64>,
        /// Emit machine-readable JSON output
        #[arg(long)]
        json: bool,
    },
}

pub async fn run(args: ComposioArgs) -> Result<()> {
    if let ComposioCommand::Execute { input, .. } = &args.command {
        let value: serde_json::Value = serde_json::from_str(input)
            .map_err(|err| anyhow::anyhow!("--input is not JSON: {err}"))?;
        if !value.is_object() {
            bail!("--input must be a JSON object");
        }
    }
    let client = connect(&args.address).await?;
    match args.command {
        ComposioCommand::List { json } => {
            let tools = client
                .list_tools(ListToolsOptions::default())
                .await
                .map_err(|err| anyhow::anyhow!(err))?;
            if json {
                println!("{}", serde_json::to_string_pretty(&tools_json(&tools))?);
            } else if tools.is_empty() {
                println!("no Composio tools");
            } else {
                for tool in &tools {
                    print_tool(tool);
                }
            }
        }
        ComposioCommand::Get { name, json } => {
            let tool = client
                .get_tool(&name)
                .await
                .map_err(|err| anyhow::anyhow!(err))?;
            if json {
                println!("{}", serde_json::to_string_pretty(&tool_json(&tool))?);
            } else {
                print_tool(&tool);
            }
        }
        ComposioCommand::Finalize { ids, json } => {
            let entries: Vec<FinalizeTool> = if ids.is_empty() {
                client
                    .list_tools(ListToolsOptions {
                        include_schemas: false,
                        include_disabled: false,
                    })
                    .await
                    .map_err(|err| anyhow::anyhow!(err))?
                    .into_iter()
                    .map(|tool| FinalizeTool {
                        id: tool.id,
                        name_override: None,
                        description_override: None,
                    })
                    .collect()
            } else {
                ids.into_iter()
                    .map(|id| FinalizeTool {
                        id,
                        name_override: None,
                        description_override: None,
                    })
                    .collect()
            };
            let outcome = client
                .finalize(&entries)
                .await
                .map_err(|err| anyhow::anyhow!(err))?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "success": outcome.success,
                        "message": outcome.message,
                        "tools": tools_json(&outcome.tools),
                    }))?
                );
            } else {
                println!(
                    "{} {}",
                    if outcome.success { "ok" } else { "failed" },
                    outcome.message
                );
                for tool in &outcome.tools {
                    print_tool(tool);
                }
            }
            if !outcome.success {
                bail!("finalize rejected the tool set");
            }
        }
        ComposioCommand::Execute {
            name,
            input,
            timeout_ms,
            json,
        } => {
            let value: serde_json::Value = serde_json::from_str(&input)
                .map_err(|err| anyhow::anyhow!("--input is not JSON: {err}"))?;
            if !value.is_object() {
                bail!("--input must be a JSON object");
            }
            let result = client
                .execute(
                    &name,
                    &value,
                    ExecuteOptions {
                        timeout_ms,
                        call_id: None,
                    },
                )
                .await
                .map_err(|err| anyhow::anyhow!(err))?;
            match result {
                CallResult::Ok {
                    call_id,
                    data,
                    prompt_text,
                    truncated,
                    duration_ms,
                } => {
                    if json {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&serde_json::json!({
                                "ok": true,
                                "call_id": call_id,
                                "data": data,
                                "prompt_text": prompt_text,
                                "truncated": truncated,
                                "duration_ms": duration_ms,
                            }))?
                        );
                    } else if prompt_text.is_empty() {
                        println!("{}", serde_json::to_string_pretty(&data)?);
                    } else {
                        println!("{prompt_text}");
                    }
                }
                CallResult::Err {
                    call_id,
                    code,
                    message,
                    retryable,
                    suggestion,
                    details,
                } => {
                    if json {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&serde_json::json!({
                                "ok": false,
                                "call_id": call_id,
                                "code": code,
                                "message": message,
                                "retryable": retryable,
                                "suggestion": suggestion,
                                "details": details,
                            }))?
                        );
                    }
                    bail!("{code}: {message}");
                }
            }
        }
    }
    Ok(())
}

async fn connect(address_flag: &Option<String>) -> Result<ComposioGrpcClient> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let address_env = address_flag
        .as_deref()
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .or_else(|| {
            std::env::var(ADDRESS_ENV)
                .ok()
                .filter(|value| !value.is_empty())
        });
    let token = std::env::var(TOKEN_ENV)
        .ok()
        .filter(|value| !value.is_empty());
    let target = resolve_target(&cwd, address_env.as_deref(), token.as_deref())
        .map_err(|err| anyhow::anyhow!(err))?;
    ComposioGrpcClient::connect(&target)
        .await
        .map_err(|err| anyhow::anyhow!(err))
}

fn print_tool(tool: &ComposioTool) {
    let state = if tool.enabled { "enabled" } else { "disabled" };
    println!("{}  {}  {state}", tool.slug, tool.id);
    if !tool.description.is_empty() {
        println!("  {}", tool.description);
    }
}

fn tools_json(tools: &[ComposioTool]) -> serde_json::Value {
    serde_json::Value::Array(tools.iter().map(tool_json).collect())
}

fn tool_json(tool: &ComposioTool) -> serde_json::Value {
    serde_json::json!({
        "slug": tool.slug,
        "id": tool.id,
        "name": tool.name,
        "description": tool.description,
        "enabled": tool.enabled,
        "version": tool.version,
        "input_schema": tool.input_schema,
        "output_schema": tool.output_schema,
    })
}
