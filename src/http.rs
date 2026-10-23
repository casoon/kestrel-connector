//! Shared HTTP agent for all providers.
//!
//! `ureq::get`/`ureq::post` use an agent without any timeout: a connection that dies silently
//! (e.g. while the machine sleeps) blocks the read forever. A caller that waits on such a
//! request (e.g. a periodic job) stands still with it. Every request goes
//! through [`agent`] instead, so a call ends with an error after [`TIMEOUT`] at the latest.

use std::sync::OnceLock;
use std::time::Duration;

/// Upper bound for one request including the response body.
pub(crate) const TIMEOUT: Duration = Duration::from_secs(60);

pub(crate) fn agent() -> &'static ureq::Agent {
    static AGENT: OnceLock<ureq::Agent> = OnceLock::new();
    AGENT.get_or_init(|| {
        ureq::Agent::config_builder()
            .timeout_global(Some(TIMEOUT))
            .build()
            .into()
    })
}
