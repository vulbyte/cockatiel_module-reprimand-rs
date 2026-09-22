# Cockatiel $mod module

Chat-command module. Parses `!$mod <user> <reason>` (engine command system) and
rates the target via the engine's `chat_$mod` query.

- Commend: unlimited.
- Reprimand: one per giver → recipient per 24h (enforced in the user-db
  `rating_history` table; the user-db cooldown test lives in
  `cockatiel_user_database-rs`).

Registers its command with the engine via `commands_payload`; receives routed
command messages as pre-process with the parsed command attached.

Build: `cargo build --release`. Run via the Cockatiel TUI (or
`cargo run --release` with `COCKATIEL_PIN` set + a `config.json`).
