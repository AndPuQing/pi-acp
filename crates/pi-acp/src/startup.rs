//! Startup info assembly + version check.
//!
//! S6 (W-453): build the startup prelude (context/skills/prompts/extensions).
//! The version/update check is **async and disabled by default** (decision 2,
//! fixes #72). Ports `buildStartupInfo` / `buildUpdateNotice` from `acp/agent.ts`.
