//! pi RPC protocol types (serde).
//!
//! Commands: prompt / abort / get_state / get_available_models / set_model /
//! set_thinking_level / set_*_mode / compact / set_auto_compaction /
//! get_session_stats / set_session_name / export_html / switch_session /
//! get_messages / get_commands, plus `extension_ui_response`.
//!
//! Events are modeled as an `enum` so the match is exhaustive at compile time.
//! Built in S3 (W-450).
