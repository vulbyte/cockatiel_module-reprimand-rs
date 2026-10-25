use futures_util::{SinkExt, StreamExt};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
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
const DEFAULT_DATA_DIR: &str = "~/.cockatiel/reprimand";
const DEFAULT_RECONNECT_BASE_SECS: u32 = 1;
const DEFAULT_RECONNECT_MAX_SECS: u32 = 30;
const MODULE_BANS_QUERY: &str = "module_bans_query";
const MODULE_BANS_REPORT: &str = "module_bans_report";
const DM_DB: &str = "_dm";

/// Module config convention: settings live in config.json's `module_specific`
/// and are created (with defaults) when missing. Reads the configured command
/// flag (e.g. "!") plus the reconnect backoff bounds, backfilling defaults for
/// any missing key.
#[derive(Debug, Clone)]
struct ModuleSettings {
    command_flag: String,
    bans_path: String,
    reconnect_base_secs: u32,
    reconnect_max_secs: u32,
}

/// Expand a leading `~` to `$HOME` (module config paths are written by hand).
fn expand_tilde(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return format!("{home}/{rest}");
        }
    } else if path == "~" {
        if let Ok(home) = std::env::var("HOME") {
            return home;
        }
    }
    path.to_string()
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
    let data_dir = ms
        .get("data_dir")
        .and_then(|v| v.as_str())
        .unwrap_or(DEFAULT_DATA_DIR)
        .to_string();
    let bans_path = ms
        .get("bans_path")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| format!("{}/bans.json", data_dir.trim_end_matches('/')));
    let reconnect_base_secs = ms
        .get("reconnect_base_secs")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
        .unwrap_or(DEFAULT_RECONNECT_BASE_SECS);
    let reconnect_max_secs = ms
        .get("reconnect_max_secs")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
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
                ms.entry("data_dir".to_string())
                    .or_insert_with(|| serde_json::json!(data_dir));
                ms.entry("bans_path".to_string())
                    .or_insert_with(|| serde_json::json!(bans_path));
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
        bans_path,
        reconnect_base_secs,
        reconnect_max_secs,
    }
}

// ── bans.json ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct BanEntry {
    user: String,
    key: String,
    by: String,
    reason: String,
    at: i64,
}

type BanMap = HashMap<String, HashMap<String, BanEntry>>;

fn load_bans(path: &str) -> BanMap {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|d| serde_json::from_str::<BanMap>(&d).ok())
        .unwrap_or_default()
}

fn save_bans(path: &str, map: &BanMap) -> Result<(), String> {
    if let Some(parent) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let json = serde_json::to_string_pretty(map).map_err(|e| e.to_string())?;
    let tmp = format!("{path}.tmp");
    std::fs::write(&tmp, json).map_err(|e| e.to_string())?;
    std::fs::rename(&tmp, path).map_err(|e| e.to_string())
}

fn normalize_ban_user(tok: &str) -> String {
    let t = tok.trim();
    if let Some(inner) = t.strip_prefix("<@") {
        let inner = inner.trim_end_matches('>');
        let inner = inner.trim_start_matches(['!', '&']);
        return inner.trim().to_string();
    }
    t.trim_start_matches('@').trim().to_string()
}

fn all_ban_ids(map: &BanMap) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for guild in map.values() {
        for entry in guild.values() {
            let id = entry.user.trim();
            if !id.is_empty() && seen.insert(id.to_string()) {
                out.push(id.to_string());
            }
        }
    }
    out
}

fn is_banned_in_guild(map: &BanMap, guild: &str, candidates: &[&str]) -> bool {
    let Some(banned) = map.get(guild) else {
        return false;
    };
    candidates.iter().any(|c| {
        let c = c.trim();
        !c.is_empty() && banned.values().any(|b| b.user == c)
    })
}

fn guild_key(chat: &ChatMessage) -> String {
    let guild = serde_json::from_slice::<serde_json::Value>(&chat.raw_data)
        .ok()
        .and_then(|v| v.get("guild_id").and_then(|x| x.as_str()).map(str::to_string))
        .unwrap_or_default();
    if guild.is_empty() {
        DM_DB.to_string()
    } else {
        guild
    }
}

fn discord_author(raw_data: &[u8]) -> (String, String) {
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(raw_data) else {
        return (String::new(), String::new());
    };
    let author = v.get("author");
    let id = author
        .and_then(|a| a.get("id"))
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    let username = author
        .and_then(|a| a.get("username"))
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    (id, username)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// mtime-cached ban file: a hand edit is picked up on the next message.
struct BanStore {
    path: String,
    cache: Option<BanMap>,
    mtime: Option<std::time::SystemTime>,
}

impl BanStore {
    fn new(path: &str) -> Self {
        Self {
            path: expand_tilde(path),
            cache: None,
            mtime: None,
        }
    }

    fn path(&self) -> &str {
        &self.path
    }

    fn bans(&mut self) -> &BanMap {
        let mtime = std::fs::metadata(&self.path)
            .and_then(|m| m.modified())
            .ok();
        if self.cache.is_none() || mtime != self.mtime {
            self.cache = Some(load_bans(&self.path));
            self.mtime = mtime;
        }
        self.cache.as_ref().unwrap()
    }

    fn invalidate(&mut self) {
        self.cache = None;
        self.mtime = None;
    }
}

fn bans_report_container(id: &EngineIdentity, map: &BanMap) -> ContainerForEngine {
    let json = serde_json::json!({ "bans": all_ban_ids(map) }).to_string();
    ContainerForEngine {
        version: 2,
        auth_token: id.auth.clone(),
        module_name: id.module.clone(),
        module_instance_uuid7: id.instance.clone(),
        payload: Some(EnginePayload::DatabaseQuery(DatabaseQuery {
            query_id: MODULE_BANS_REPORT.to_string(),
            sql: json,
            params: Vec::new(),
        })),
    }
}

fn ban_actor(chat: &ChatMessage) -> (bool, String) {
    let Some(ud) = chat.user_data.as_ref() else {
        return (false, chat.user_uuid7.clone());
    };
    let allowed = ud.is_moderator || ud.is_admin || ud.is_owner;
    let actor = if !ud.uuid.is_empty() {
        ud.uuid.clone()
    } else if !ud.username.is_empty() {
        ud.username.clone()
    } else {
        chat.user_uuid7.clone()
    };
    (allowed, actor)
}

fn reply_container(id: &EngineIdentity, chat: &ChatMessage, msg: String) -> ContainerForEngine {
    let handle = chat
        .user_data
        .as_ref()
        .map(|u| u.username.clone())
        .unwrap_or_default();
    ContainerForEngine {
        version: 2,
        auth_token: id.auth.clone(),
        module_name: id.module.clone(),
        module_instance_uuid7: id.instance.clone(),
        payload: Some(EnginePayload::SendToPlatforms(SendToPlatforms {
            msg,
            level: 0,
            module_uuid7: id.instance.clone(),
            pid: String::new(),
            platform: chat.platform.clone(),
            actor_platform: chat.platform.clone(),
            actor_handle: handle,
            actor_uuid7: chat.user_uuid7.clone(),
            channel_id: chat.channel_id.clone(),
        })),
    }
}

#[derive(Debug, PartialEq)]
enum BanCmd {
    Ban { user: String, reason: String },
    Unban(String),
}

fn parse_ban_subcommand(args: &str) -> Option<BanCmd> {
    let args = args.trim();
    let (head, rest) = match args.split_once(char::is_whitespace) {
        Some((h, r)) => (h, r.trim()),
        None => (args, ""),
    };
    match head.to_ascii_lowercase().as_str() {
        "ban" => {
            let (user, reason) = match rest.split_once(char::is_whitespace) {
                Some((u, r)) => (u, r.trim()),
                None => (rest, ""),
            };
            (!user.is_empty()).then(|| BanCmd::Ban {
                user: user.to_string(),
                reason: reason.to_string(),
            })
        }
        "unban" => {
            let target = rest.split_whitespace().next().unwrap_or("");
            (!target.is_empty()).then(|| BanCmd::Unban(target.to_string()))
        }
        _ => None,
    }
}

enum BanOutcome {
    Reply(String),
    Report(String, BanMap),
}

fn handle_bans_command(
    chat: &ChatMessage,
    args: &str,
    store: &mut BanStore,
    label: &str,
) -> Option<BanOutcome> {
    let cmd = parse_ban_subcommand(args)?;
    let guild = guild_key(chat);
    let (allowed, actor) = ban_actor(chat);
    if !allowed {
        return Some(BanOutcome::Reply(format!(
            "Only moderators can manage {label} bans."
        )));
    }
    let mut map = load_bans(store.path());
    let outcome = match cmd {
        BanCmd::Ban { user, reason } => {
            let target = normalize_ban_user(&user);
            if target.is_empty() {
                return Some(BanOutcome::Reply(
                    "Usage: `ban <user> [reason]`.".to_string(),
                ));
            }
            let entry = BanEntry {
                user: target.clone(),
                key: target.clone(),
                by: actor,
                reason: reason.clone(),
                at: now_ms(),
            };
            map.entry(guild.clone()).or_default().insert(target.clone(), entry);
            match save_bans(store.path(), &map) {
                Ok(()) => {
                    store.invalidate();
                    if reason.is_empty() {
                        BanOutcome::Report(format!("Banned `{target}` from {label}."), map)
                    } else {
                        BanOutcome::Report(
                            format!("Banned `{target}` from {label}: {reason}"),
                            map,
                        )
                    }
                }
                Err(e) => BanOutcome::Reply(format!("Could not save the ban: {e}")),
            }
        }
        BanCmd::Unban(user) => {
            let target = normalize_ban_user(&user);
            let removed = map
                .get_mut(&guild)
                .and_then(|g| g.remove(&target))
                .is_some();
            if removed && map.get(&guild).map(|g| g.is_empty()).unwrap_or(false) {
                map.remove(&guild);
            }
            match save_bans(store.path(), &map) {
                Ok(()) => {
                    store.invalidate();
                    if removed {
                        BanOutcome::Report(format!("Unbanned `{target}` from {label}."), map)
                    } else {
                        BanOutcome::Reply(format!("`{target}` was not banned."))
                    }
                }
                Err(e) => BanOutcome::Reply(format!("Could not save the ban list: {e}")),
            }
        }
    };
    Some(outcome)
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
        let mut bans = BanStore::new(&settings.bans_path);
        loop {
            // Fresh session (initial connect + every reconnect): register the
            // command — the engine forgets commands when a socket drops.
            register_commands(&write_for_task, &identity_for_task, &command_flag_for_task).await;
            // Announce our current bans so the engine learns them immediately.
            {
                let id = identity_for_task.lock().await.clone();
                let report = {
                    let map = bans.bans();
                    bans_report_container(&id, map)
                };
                send_container(&write_for_task, report).await;
            }
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
                        if res.query_id == MODULE_BANS_QUERY {
                            let report = {
                                let map = bans.bans();
                                bans_report_container(&id, map)
                            };
                            send_container(&write_for_task, report).await;
                        } else if !res.success {
                            // Surface query failures (cooldown denial, target not
                            // found, self-rating rejection) so the operator sees
                            // them; no platform reply is sent.
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
                        if !uuid.is_empty() {
                            let receipt = ContainerForEngine {
                                version: 2,
                                auth_token: id.auth.clone(),
                                module_name: id.module.clone(),
                                module_instance_uuid7: id.instance.clone(),
                                payload: Some(EnginePayload::MessageAck(MessageAck {
                                    message_uuid7: uuid.clone(),
                                })),
                            };
                            send_container(&write_for_task, receipt).await;
                        }
                        // Handle our routed command when present.
                        if let Some(chat) = &raw_message {
                            let guild = guild_key(chat);
                            let (author_id, author_name) = discord_author(&chat.raw_data);
                            let locally_banned = is_banned_in_guild(
                                bans.bans(),
                                &guild,
                                &[chat.user_uuid7.as_str(), author_id.as_str(), author_name.as_str()],
                            );
                            if locally_banned {
                                // Local enforcement: ignore banned senders entirely.
                            } else if let Some(cmd) = &chat.command {
                                if cmd.command_name == COMMAND_NAME {
                                    let args =
                                        strip_command(&chat.raw_message, &cmd.command_flag, &cmd.command_name);
                                    if let Some(outcome) = handle_bans_command(chat, &args, &mut bans, "reprimand") {
                                        match outcome {
                                            BanOutcome::Reply(msg) => {
                                                send_container(&write_for_task, reply_container(&id, chat, msg)).await;
                                            }
                                            BanOutcome::Report(msg, map) => {
                                                send_container(&write_for_task, reply_container(&id, chat, msg)).await;
                                                send_container(&write_for_task, bans_report_container(&id, &map)).await;
                                            }
                                        }
                                    } else {
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
                        if !process.message_uuid7.is_empty() {
                            let receipt = ContainerForEngine {
                                version: 2,
                                auth_token: id.auth.clone(),
                                module_name: id.module.clone(),
                                module_instance_uuid7: id.instance.clone(),
                                payload: Some(EnginePayload::MessageAck(MessageAck {
                                    message_uuid7: process.message_uuid7.clone(),
                                })),
                            };
                            send_container(&write_for_task, receipt).await;
                        }
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
                    Some(ModulePayload::Shutdown(_)) => {
                        info!("Engine requested shutdown — exiting");
                        std::process::exit(0);
                    }
                    _ => {}
                }
            }

            // The engine connection dropped — reconnect with backoff instead of
            // leaving the module unresponsive.
            info!("Engine disconnected — reconnecting...");
            let mut backoff = reconnect_base_secs;
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(backoff as u64)).await;
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

    fn mod_chat(text: &str, guild: &str) -> ChatMessage {
        ChatMessage {
            platform: "discord".into(),
            raw_data: format!(r#"{{"guild_id":"{guild}"}}"#).into_bytes(),
            raw_message: text.into(),
            user_uuid7: "mod".into(),
            command: None,
            user_data: Some(UserData {
                uuid: "mod-uuid".into(),
                username: "mod".into(),
                is_moderator: true,
                ..Default::default()
            }),
            channel_id: "c".into(),
        }
    }

    #[test]
    fn parses_ban_and_unban_subcommands() {
        assert_eq!(
            parse_ban_subcommand("ban @bob spam"),
            Some(BanCmd::Ban {
                user: "@bob".into(),
                reason: "spam".into()
            })
        );
        assert_eq!(
            parse_ban_subcommand("unban <@123>"),
            Some(BanCmd::Unban("<@123>".into()))
        );
        assert_eq!(parse_ban_subcommand("ban"), None);
        assert_eq!(parse_ban_subcommand("unban"), None);
        assert_eq!(parse_ban_subcommand("@user rude"), None);
        assert_eq!(normalize_ban_user("@bob"), "bob");
        assert_eq!(normalize_ban_user("<@!123>"), "123");
    }

    #[test]
    fn ban_command_persists_and_enforces_locally() {
        let dir = std::env::temp_dir().join(format!("cockatiel-reprimand-{}", uuid::Uuid::now_v7()));
        let path = dir.join("bans.json");
        let mut store = BanStore::new(path.to_string_lossy().as_ref());
        let chat = mod_chat("!reprimand ban @badguy rude", "guild1");
        match handle_bans_command(&chat, "ban @badguy rude", &mut store, "reprimand").unwrap() {
            BanOutcome::Report(msg, map) => {
                assert!(msg.contains("Banned `badguy`"));
                assert_eq!(all_ban_ids(&map), vec!["badguy".to_string()]);
            }
            BanOutcome::Reply(m) => panic!("unexpected reply: {m}"),
        }
        let map = load_bans(store.path());
        assert_eq!(map["guild1"]["badguy"].by, "mod-uuid");
        assert_eq!(map["guild1"]["badguy"].reason, "rude");
        assert!(is_banned_in_guild(store.bans(), "guild1", &["badguy"]));
        assert!(!is_banned_in_guild(store.bans(), "guild2", &["badguy"]));

        let mut norm = mod_chat("!reprimand ban @other", "guild1");
        norm.user_data = Some(UserData {
            uuid: "u".into(),
            username: "someone".into(),
            ..Default::default()
        });
        match handle_bans_command(&norm, "ban @other", &mut store, "reprimand").unwrap() {
            BanOutcome::Reply(msg) => assert!(msg.contains("Only moderators")),
            _ => panic!("non-mod must not ban"),
        }

        match handle_bans_command(&chat, "unban badguy", &mut store, "reprimand").unwrap() {
            BanOutcome::Report(msg, _) => assert!(msg.contains("Unbanned `badguy`")),
            BanOutcome::Reply(m) => panic!("unexpected reply: {m}"),
        }
        assert!(!load_bans(store.path()).contains_key("guild1"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
