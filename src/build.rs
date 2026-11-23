//! The build id of this crate, set by `build.rs`: which commit every binary
//! of the crate was built from, and when.
//!
//! A build is named by the pair ([`GIT`], [`UNIX`]): two builds of one dirty
//! tree differ in [`UNIX`]. `av-mesh --version` prints [`VERSION`], and a
//! node's `GET /api/node` carries the parts.

/// The commit: `git rev-parse HEAD` (40 hex digits) when the build ran, or
/// `NEEDLETAIL_BUILD_GIT` when that was set, or `unknown`.
pub const GIT_FULL: &str = env!("AV_MESH_BUILD_GIT_FULL");

/// Whether `git status --porcelain` listed anything when the build ran.
pub const DIRTY: bool = matches!(env!("AV_MESH_BUILD_DIRTY").as_bytes(), b"true");

/// When the build ran, in Unix seconds: `SOURCE_DATE_EPOCH` when that was
/// set, else the time `build.rs` ran.
pub const UNIX: u64 = decimal(env!("AV_MESH_BUILD_UNIX"));

/// The first 12 characters of [`GIT_FULL`], with `-dirty` after them when
/// the tree was dirty: `3f2a9c1d77e0`, `3f2a9c1d77e0-dirty`. Safe in a path.
pub const GIT: &str = env!("AV_MESH_BUILD_GIT");

/// The crate's version without the build: `0.1.0`.
pub const PACKAGE: &str = env!("CARGO_PKG_VERSION");

/// What `--version` prints after the binary's name:
/// `0.1.0 (3f2a9c1d77e0 1763900000)`.
pub const VERSION: &str = env!("AV_MESH_BUILD_VERSION");

const fn decimal(text: &str) -> u64 {
    let digits = text.as_bytes();
    let mut value = 0_u64;
    let mut index = 0;
    while index < digits.len() {
        assert!(digits[index].is_ascii_digit(), "the build time is decimal");
        value = value * 10 + (digits[index] - b'0') as u64;
        index += 1;
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_version_names_the_commit_and_the_time() {
        assert_eq!(VERSION, format!("{PACKAGE} ({GIT} {UNIX})"));
        assert!(UNIX > 0);
        let short: String = GIT_FULL.chars().take(12).collect();
        let suffix = if DIRTY { "-dirty" } else { "" };
        assert_eq!(GIT, format!("{short}{suffix}"));
        // The build id names a directory on a node and is one word of
        // `--version`.
        assert!(GIT
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')));
    }
}
