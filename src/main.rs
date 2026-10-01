use futures_util::{SinkExt, StreamExt};
use prost::Message;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;
use tracing::{info, warn};
use tracing_subscriber::FmtSubscriber;

use cockatiel_client::proto::container_for_engine::Payload as EnginePayload;
use cockatiel_client::proto::container_for_module::Payload as ModulePayload;
use cockatiel_client::proto::*;
use cockatiel_client::CockatielClient;

type WsWriteHalf = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    WsMessage,
>;

const COMMAND_NAME: &str = "reprimand";
const DEFAULT_FLAG: &str = "!";
const DEFAULT_RECONNECT_BASE_SECS: u64 = 1;
const DEFAULT_RECONNECT_MAX_SECS: u64 = 30;

/// Module config convention: settings live in config.json's `module_specific`
/// and are created (with defaults) when missing. Reads the configured command
/// flag (e.g. "!") plus the reconnect backoff bounds, backfilling defaults for
/// any missing key.
#[derive(Debug, Clone)]
struct ModuleSettings {
    command_flag: String,
    reconnect_base_secs: u64,
    reconnect_max_secs: u64,
}

fn ensure_defaults() -> ModuleSettings {
    let root: Option<serde_json::Value> = std::fs::read_to_string("config.json")
        .ok()
        .and_then(|data| serde_json::from_str(&data).ok());
    let ms = root
        .as_ref()
        .and_then(|r| r.get("module_specific"))
        .and_then(|ms| ms.as_object())
        .cloned()
        .unwrap_or_default();
    let command_flag = ms
        .get("command_flag")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| DEFAULT_FLAG.to_string());
    let reconnect_base_secs = ms
        .get("reconnect_base_secs")
        .and_then(|v| v.as_u64())
        .unwrap_or(DEFAULT_RECONNECT_BASE_SECS);
    let reconnect_max_secs = ms
        .get("reconnect_max_secs")
        .and_then(|v| v.as_u64())
        .unwrap_or(DEFAULT_RECONNECT_MAX_SECS);
    if let Some(mut root) = root {
        if let Some(obj) = root.as_object_mut() {
            if let Some(ms) = obj
                .entry("module_specific".to_string())
                .or_insert_with(|| serde_json::json!({}))
                .as_object_mut()
            {
                ms.entry("command_flag".to_string())
                    .or_insert_with(|| serde_json::json!(command_flag));
                ms.entry("reconnect_base_secs".to_string())
                    .or_insert_with(|| serde_json::json!(reconnect_base_secs));
                ms.entry("reconnect_max_secs".to_string())
                    .or_insert_with(|| serde_json::json!(reconnect_max_secs));
            }
            let _ = std::fs::write("config.json", serde_json::to_string_pretty(&root).unwrap());
        }
    }
    ModuleSettings {
        command_flag,
        reconnect_base_secs,
        reconnect_max_secs,
    }
}

/// Session identity (auth token + module ids) shared between the read loop and
/// the query sender so a reconnect's fresh credentials are picked up by both.
#[derive(Clone)]
struct EngineIdentity {
    auth: String,
    instance: String,
    module: String,
}

/// Encode and send a ContainerForEngine on the shared write half.
async fn send_container(write_shared: &Arc<AsyncMutex<WsWriteHalf>>, container: ContainerForEngine) {
    let mut buf = Vec::new();
    if container.encode(&mut buf).is_ok() {
        let mut w = write_shared.lock().await;
        let _ = w.send(WsMessage::Binary(buf)).await;
    }
}

/// Register the chat command with the engine. Called on every fresh session
/// (initial connect and each reconnect — the engine forgets a session's
/// commands when the socket drops).
async fn register_commands(
    write_shared: &Arc<AsyncMutex<WsWriteHalf>>,
    identity: &Arc<AsyncMutex<EngineIdentity>>,
    command_flag: &str,
) {
    let id = identity.lock().await.clone();
    let commands = ContainerForEngine {
        version: 2,
        auth_token: id.auth,
        module_name: id.module,
        module_instance_uuid7: id.instance,
        payload: Some(EnginePayload::Commands(Commands {
            commands: vec![Command {
                command_name: COMMAND_NAME.to_string(),
                command_flag: command_flag.to_string(),
                command_description: "reprimand a user (once per 24h per person)".to_string(),
                command_flags: vec![],
            }],
            alert_on_unknown_command: false,
        })),
    };
    send_container(write_shared, commands).await;
    info!("registered !{} command", COMMAND_NAME);
}

/// Extract the positional args (target + reason) from a routed command. The
/// engine already parsed the command (it attached `chat.command`), so we only
/// strip the `<flag><command>` prefix and keep everything after verbatim. This
/// module registers NO flags, so a `-word` inside the reason is reason text,
/// not a flag — the old dash-scanning heuristic (which dropped a token AFTER a
/// `-flag` token) is gone.
fn strip_command(raw: &str, flag: &str, name: &str) -> String {
    let mut rest = raw.trim().to_string();
    if let Some(idx) = rest.find(flag) {
        rest = rest[idx + flag.len()..].trim_start().to_string();
    }
    if rest.starts_with(name) {
        rest = rest[name.len()..].trim_start().to_string();
    }
    rest
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let client = CockatielClient::connect("config.json").await?;
    let (write, read) = client.stream.split();
    let write_shared: Arc<AsyncMutex<WsWriteHalf>> = Arc::new(AsyncMutex::new(write));
    let module_name = client.config.module_name.clone();
    info!("reprimand module connected as '{}'", module_name);
    let identity: Arc<AsyncMutex<EngineIdentity>> = Arc::new(AsyncMutex::new(EngineIdentity {
        auth: client.auth_token.clone(),
        instance: client.instance_uuid7.clone(),
        module: module_name,
    }));

    // Channel carrying parsed ratings to the (serialized) engine query sender.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<serde_json::Value>();

    // Read loop + engine-session supervisor: owns the read half, answers
    // liveness probes, acks every pre-process/in-process stage, surfaces
    // DatabaseQueryResult failures, and reconnects with backoff — re-registering
    // the command on each fresh session.
    let settings = ensure_defaults();
    let command_flag = settings.command_flag.clone();
    let reconnect_base_secs = settings.reconnect_base_secs;
    let reconnect_max_secs = settings.reconnect_max_secs;
    let write_for_task = Arc::clone(&write_shared);
    let identity_for_task = Arc::clone(&identity);
    let tx_for_task = tx.clone();
    let command_flag_for_task = command_flag.clone();
    tokio::spawn(async move {
        let mut read = read;
        loop {
            // Fresh session (initial connect + every reconnect): register the
            // command — the engine forgets commands when a socket drops.
            register_commands(&write_for_task, &identity_for_task, &command_flag_for_task).await;
            loop {
                let Some(msg) = read.next().await else { break };
                let data = match msg {
                    Ok(WsMessage::Binary(d)) => d,
                    Ok(WsMessage::Close(_)) => {
                        info!("Engine closed connection");
                        break;
                    }
                    Ok(_) => continue,
                    Err(e) => {
                        warn!("Engine WebSocket error: {}", e);
                        break;
                    }
                };
                let Ok(container) = ContainerForModule::decode(data.as_ref()) else { continue };
                let id = identity_for_task.lock().await.clone();
                match container.payload {
                    Some(ModulePayload::AuthVerify(_)) => {
                        let reply = ContainerForEngine {
                            version: 2,
                            auth_token: id.auth.clone(),
                            module_name: id.module.clone(),
                            module_instance_uuid7: id.instance.clone(),
                            payload: Some(EnginePayload::AuthVerify(AuthVerify {
                                cur_auth: id.auth.clone(),
                            })),
                        };
                        send_container(&write_for_task, reply).await;
                    }
                    Some(ModulePayload::DatabaseQueryResult(res)) => {
                        // Surface query failures (cooldown denial, target not
                        // found, self-rating rejection) so the operator sees
                        // them; no platform reply is sent.
                        if !res.success {
                            eprintln!(
                                "[reprimand] rating query '{}' failed: {}",
                                res.query_id, res.error
                            );
                        }
                    }
                    Some(ModulePayload::MessagePreProcess(pre)) => {
                        let MessagePreProcess {
                            message_uuid7: uuid,
                            raw_message,
                            audio,
                            audio_type,
                        } = pre;
                        // Handle our routed command when present.
                        if let Some(chat) = &raw_message {
                            if let Some(cmd) = &chat.command {
                                if cmd.command_name == COMMAND_NAME {
                                    let args =
                                        strip_command(&chat.raw_message, &cmd.command_flag, &cmd.command_name);
                                    let mut parts = args.split_whitespace();
                                    if let Some(target) = parts.next() {
                                        let reason = parts.collect::<Vec<_>>().join(" ");
                                        let target_clean = target
                                            .strip_prefix('@')
                                            .unwrap_or(target)
                                            .to_string();
                                        let _ = tx_for_task.send(serde_json::json!({
                                            "platform": chat.platform,
                                            "handle": target_clean,
                                            "reason": reason,
                                            "actor": {
                                                "uuid7": chat.user_uuid7,
                                                "platform": chat.platform,
                                                "handle": chat.user_data.as_ref().map(|u| u.username.clone()).unwrap_or_default(),
                                            },
                                        }));
                                    }
                                }
                            }
                        }
                        // ALWAYS ack the pre-process stage: echo the raw
                        // ChatMessage back with the same message_uuid7 so the
                        // engine advances instead of stalling until the timeout
                        // sweep — on every path, command or not.
                        let ack = ContainerForEngine {
                            version: 2,
                            auth_token: id.auth.clone(),
                            module_name: id.module.clone(),
                            module_instance_uuid7: id.instance.clone(),
                            payload: Some(EnginePayload::MessagePreProcess(MessagePreProcess {
                                message_uuid7: uuid,
                                raw_message,
                                audio,
                                audio_type,
                            })),
                        };
                        send_container(&write_for_task, ack).await;
                    }
                    Some(ModulePayload::MessageInProcess(process)) => {
                        // Pass-through ack of the in-process stage so it never
                        // stalls (even though this module only declares
                        // pre-process capability).
                        let ack = ContainerForEngine {
                            version: 2,
                            auth_token: id.auth.clone(),
                            module_name: id.module.clone(),
                            module_instance_uuid7: id.instance.clone(),
                            payload: Some(EnginePayload::MessageInProcess(MessageInProcess {
                                message_uuid7: process.message_uuid7,
                                raw_message: process.raw_message,
                                processed_message: process.processed_message,
                                abandon_message: process.abandon_message,
                                audio: process.audio,
                                audio_type: process.audio_type,
                            })),
                        };
                        send_container(&write_for_task, ack).await;
                    }
                    _ => {}
                }
            }

            // The engine connection dropped — reconnect with backoff instead of
            // leaving the module unresponsive.
            info!("Engine disconnected — reconnecting...");
            let mut backoff = reconnect_base_secs;
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
                match CockatielClient::connect("config.json").await {
                    Ok(conn) => {
                        info!("Reconnected to engine");
                        let (w, r) = conn.stream.split();
                        *write_for_task.lock().await = w;
                        *identity_for_task.lock().await = EngineIdentity {
                            auth: conn.auth_token,
                            instance: conn.instance_uuid7,
                            module: conn.config.module_name,
                        };
                        read = r;
                        break; // back to outer loop → re-register the command
                    }
                    Err(e) => {
                        warn!("Engine reconnect failed: {} — retrying in {}s", e, backoff);
                        backoff = (backoff * 2).min(reconnect_max_secs);
                    }
                }
            }
        }
    });

    // Serialize the ratings: each one goes to the engine's chat_reprimand query.
    while let Some(payload) = rx.recv().await {
        let id = identity.lock().await.clone();
        let query = ContainerForEngine {
            version: 2,
            auth_token: id.auth,
            module_name: id.module,
            module_instance_uuid7: id.instance,
            payload: Some(EnginePayload::DatabaseQuery(DatabaseQuery {
                query_id: "chat_reprimand".to_string(),
                sql: payload.to_string(),
                params: vec![],
            })),
        };
        send_container(&write_shared, query).await;
    }

    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_flag_and_command_keeps_args() {
        assert_eq!(strip_command("!reprimand @user being rude", "!", "reprimand"), "@user being rude");
    }

    #[test]
    fn strips_mention_symbol_style() {
        assert_eq!(strip_command("!reprimand  @user", "!", "reprimand"), "@user");
    }

    #[test]
    fn preserves_flag_like_reason_text() {
        // No flags registered → `-v high` is reason text, not a flag; it is
        // preserved rather than silently dropped with its value token.
        assert_eq!(strip_command("!reprimand -v high @user rude", "!", "reprimand"), "-v high @user rude");
    }

    #[test]
    fn preserves_dash_word_in_reason_when_no_flags_parsed() {
        assert_eq!(strip_command("!reprimand @user not -half bad", "!", "reprimand"), "@user not -half bad");
    }

    #[test]
    fn empty_args_when_only_command() {
        assert_eq!(strip_command("!reprimand", "!", "reprimand"), "");
    }
}
