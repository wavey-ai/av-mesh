//! The boot check's record (D 2.4): `needletail-nodecheck boot` writes
//! `/run/needletail/node-warm.json` once a boot, before the service starts;
//! av-mesh reads it once at start (`--node-warm-file`) into `node_warm` of
//! `/api/mesh/local` and `/api/node`, and readiness keeps a node whose check
//! did not come out `complete` out (`ReadinessInputs.node_warm_complete`).

use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::OnceLock;

/// The schema `needletail-nodecheck boot` writes.
pub const SCHEMA: &str = "needletail.node-warm.v2";

/// What the node says of its boot check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeWarmStatus {
    /// A file was configured (always true where the status exists).
    pub configured: bool,
    pub complete: bool,
    /// The check's verdict (`ok`, `warm-timeout`, `stops`, `steal`), or why
    /// there is none: `missing` (no file), `unreadable` (not the schema).
    pub verdict: String,
    /// The check's `warm` and `check` objects, as written.
    pub warm: Option<serde_json::Value>,
    pub check: Option<serde_json::Value>,
}

/// The record as `needletail-nodecheck boot` writes it.
#[derive(Deserialize)]
struct Record {
    schema: String,
    complete: bool,
    verdict: String,
    #[serde(default)]
    warm: Option<serde_json::Value>,
    #[serde(default)]
    check: Option<serde_json::Value>,
}

impl NodeWarmStatus {
    fn failed(verdict: &str) -> Self {
        Self {
            configured: true,
            complete: false,
            verdict: verdict.to_owned(),
            warm: None,
            check: None,
        }
    }

    /// The status a record's text gives.
    pub fn parse(text: &str) -> Self {
        match serde_json::from_str::<Record>(text) {
            Ok(record) if record.schema == SCHEMA => Self {
                configured: true,
                // Only an `ok` check is complete, whatever the file says.
                complete: record.complete && record.verdict == "ok",
                verdict: record.verdict,
                warm: record.warm,
                check: record.check,
            },
            _ => Self::failed("unreadable"),
        }
    }

    /// The status of the file at `path`.
    pub fn read(path: &Path) -> Self {
        match std::fs::read_to_string(path) {
            Ok(text) => Self::parse(&text),
            Err(_) => Self::failed("missing"),
        }
    }
}

static INSTALLED: OnceLock<Option<NodeWarmStatus>> = OnceLock::new();

/// Read `--node-warm-file` once, at start.
pub fn install(path: Option<&Path>) {
    let status = path.map(NodeWarmStatus::read);
    if let Some(status) = &status {
        tracing::info!(
            complete = status.complete,
            verdict = status.verdict,
            "boot check record read"
        );
    }
    let _ = INSTALLED.set(status);
}

/// The status read at start; `None` when no file was configured.
pub fn installed() -> Option<&'static NodeWarmStatus> {
    INSTALLED.get().and_then(Option::as_ref)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_s_verdict_decides_complete() {
        let ok = NodeWarmStatus::parse(
            r#"{"schema":"needletail.node-warm.v2","complete":true,"verdict":"ok","unix_us":1,
               "warm":{"ran":true,"finished":true,"gib":3.31},"check":{"seconds":60,"stops_over_20ms":0}}"#,
        );
        assert!(ok.configured && ok.complete);
        assert_eq!(ok.verdict, "ok");
        assert_eq!(ok.warm.as_ref().unwrap()["gib"], 3.31);
        assert_eq!(ok.check.as_ref().unwrap()["seconds"], 60);
        let stops = NodeWarmStatus::parse(
            r#"{"schema":"needletail.node-warm.v2","complete":false,"verdict":"stops","unix_us":1,"warm":null,"check":{}}"#,
        );
        assert!(!stops.complete);
        assert_eq!(stops.verdict, "stops");
        let other = NodeWarmStatus::parse(r#"{"schema":"other","complete":true,"verdict":"ok"}"#);
        assert_eq!(
            (other.complete, other.verdict.as_str()),
            (false, "unreadable")
        );
        assert_eq!(NodeWarmStatus::parse("{").verdict, "unreadable");
        let missing = NodeWarmStatus::read(Path::new("/nonexistent/node-warm.json"));
        assert_eq!((missing.configured, missing.complete), (true, false));
        assert_eq!(missing.verdict, "missing");
    }
}
