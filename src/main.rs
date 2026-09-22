use futures_util::{SinkExt, StreamExt};
use prost::Message;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;
use tracing::info;
use tracing_subscriber::FmtSubscriber;

use cockatiel_client::{proto::container::Payload, proto::*, CockatielClient};

type WsWriteHalf = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    WsMessage,
>;

const COMMAND_NAME: &str = "reprimand";

/// Strip the `<flag><command>` prefix (and any parsed `-flag` tokens) from a
/// raw command message, leaving the positional args (target + reason).
fn strip_command(raw: &str, flag: &str, name: &str) -> String {
    let mut rest = raw.trim().to_string();
    if let Some(idx) = rest.find(flag) {
        rest = rest[idx + flag.len()..].trim_start().to_string();
    }
    if rest.starts_with(name) {
        rest = rest[name.len()..].trim_start().to_string();
    }
    // Drop any `-flag [value]` tokens — the positional args come after.
    let mut kept: Vec<&str> = Vec::new();
    let tokens: Vec<&str> = rest.split_whitespace().collect();
    let mut i = 0;
    while i < tokens.len() {
        let t = tokens[i];
        if t.starts_with('-') && t.len() > 1 {
            if i + 1 < tokens.len() && !tokens[i + 1].starts_with('-') {
                i += 1;
            }
        } else {
            kept.push(t);
        }
        i += 1;
    }
    kept.join(" ")
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
    let (write, mut read) = client.stream.split();
    let write_shared: Arc<AsyncMutex<WsWriteHalf>> = Arc::new(AsyncMutex::new(write));
    let auth_token = client.auth_token.clone();
    let instance_uuid = client.instance_uuid7.clone();
    let module_name = client.config.module_name.clone();
    info!("reprimand module connected as '{}'", module_name);

    // The engine drains frames sent within its ~40ms post-auth window; settle
    // before registering so the Commands payload isn't discarded.
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    // Register the command with the engine: `!reprimand <user> <reason>`.
    {
        let commands = Container {
            version: 1,
            auth_token: auth_token.clone(),
            module_name: module_name.clone(),
            module_instance_uuid7: instance_uuid.clone(),
            payload: Some(Payload::CommandsPayload(Commands {
                commands: vec![Command {
                    command_name: COMMAND_NAME.to_string(),
                    command_flag: "!".to_string(),
                    command_description: "reprimand a user (once per 24h per person)".to_string(),
                    command_flags: vec![],
                }],
                alert_on_unknown_command: false,
            })),
        };
        let mut buf = Vec::new();
        commands.encode(&mut buf)?;
        let mut w = write_shared.lock().await;
        w.send(WsMessage::Binary(buf.into())).await?;
        drop(w);
        info!("registered !{} command", COMMAND_NAME);
    }

    // Read task: handle routed command messages.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<serde_json::Value>();
    let read_tx = tx.clone();
    let write_for_task = Arc::clone(&write_shared);
    let auth_for_task = auth_token.clone();
    let module_for_task = module_name.clone();
    let instance_for_task = instance_uuid.clone();
    tokio::spawn(async move {
        while let Some(msg) = read.next().await {
            let Ok(WsMessage::Binary(data)) = msg else { continue };
            let Ok(container) = Container::decode(data.as_ref()) else { continue };
            match container.payload {
                Some(Payload::AuthVerify(_)) => {
                    let reply = Container {
                        version: 1,
                        auth_token: auth_for_task.clone(),
                        module_name: module_for_task.clone(),
                        module_instance_uuid7: instance_for_task.clone(),
                        payload: Some(Payload::AuthVerify(AuthVerify {
                            cur_auth: auth_for_task.clone(),
                        })),
                    };
                    let mut buf = Vec::new();
                    if reply.encode(&mut buf).is_ok() {
                        let mut w = write_for_task.lock().await;
                        let _ = w.send(WsMessage::Binary(buf.into())).await;
                    }
                }
                Some(Payload::MessagePreProcess(pre)) => {
                    let Some(chat) = pre.raw_message else { continue };
                    let Some(cmd) = chat.command else { continue };
                    if cmd.command_name != COMMAND_NAME {
                        continue; // not our command (safety: catch-alls see everything)
                    }
                    let args = strip_command(&chat.raw_message, &cmd.command_flag, &cmd.command_name);
                    let mut parts = args.split_whitespace();
                    let Some(target) = parts.next() else {
                        continue; // missing target
                    };
                    let reason = parts.collect::<Vec<_>>().join(" ");
                    let target_clean = target.strip_prefix('@').unwrap_or(target).to_string();
                    let _ = read_tx.send(serde_json::json!({
                        "platform": chat.platform,
                        "handle": target_clean,
                        "reason": reason,
                        "actor": {
                            "uuid7": chat.user_uuid7,
                            "platform": chat.platform,
                            "handle": chat.user_data.map(|u| u.username).unwrap_or_default(),
                        },
                    }));
                }
                _ => {}
            }
        }
    });

    // Serialize the ratings: each one goes to the engine's chat_reprimand query.
    while let Some(payload) = rx.recv().await {
        let query = Container {
            version: 1,
            auth_token: auth_token.clone(),
            module_name: module_name.clone(),
            module_instance_uuid7: instance_uuid.clone(),
            payload: Some(Payload::DatabaseQuery(DatabaseQuery {
                query_id: "chat_reprimand".to_string(),
                sql: payload.to_string(),
                params: vec![],
            })),
        };
        let mut buf = Vec::new();
        if query.encode(&mut buf).is_ok() {
            let mut w = write_shared.lock().await;
            let _ = w.send(WsMessage::Binary(buf.into())).await;
        }
    }

    Ok(())
}