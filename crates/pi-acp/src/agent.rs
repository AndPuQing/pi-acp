//! ACP `Agent` role implementation.
//!
//! S6 (W-453) implements: initialize / session/new / prompt / cancel / load /
//! list / delete / set_mode / set_config_option / unstable_setSessionModel,
//! configOptions (model + thought_level), `usage_update` (decision 3).
//! Handlers stay thin; logic lives in `session` + `translate`.
