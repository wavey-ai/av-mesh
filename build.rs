//! The build id of the crate (`src/build.rs`): the commit, whether the tree
//! was dirty, and when the build ran.
//!
//! `NEEDLETAIL_BUILD_GIT` names the commit in place of `git rev-parse HEAD`
//! (a build from an archive, or two builds of one tree that must differ), and
//! `SOURCE_DATE_EPOCH` the time in place of now.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8(output.stdout).ok()?.trim().to_owned())
}

/// A path as git prints it, which is relative to the package root when the
/// repository is a plain checkout.
fn rerun_if_changed(path: impl AsRef<Path>) {
    println!("cargo:rerun-if-changed={}", path.as_ref().display());
}

fn main() {
    println!("cargo:rerun-if-env-changed=NEEDLETAIL_BUILD_GIT");
    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");
    rerun_if_changed("build.rs");
    rerun_if_changed("src");
    rerun_if_changed("Cargo.lock");
    // The real HEAD and ref files: in a worktree `.git` is a file, and the
    // branch's ref is in the common directory (or in its packed-refs).
    if let Some(head) = git(&["rev-parse", "--git-path", "HEAD"]) {
        rerun_if_changed(&head);
    }
    if let Some(reference) = git(&["symbolic-ref", "-q", "HEAD"]) {
        if let Some(path) = git(&["rev-parse", "--git-path", &reference]) {
            rerun_if_changed(&path);
        }
    }
    if let Some(common) = git(&["rev-parse", "--git-common-dir"]) {
        rerun_if_changed(PathBuf::from(common).join("packed-refs"));
    }

    let named = std::env::var("NEEDLETAIL_BUILD_GIT")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    let full = named
        .or_else(|| git(&["rev-parse", "HEAD"]))
        .unwrap_or_else(|| "unknown".to_owned());
    // The id names a directory on a node and is one word of `--version`.
    assert!(
        full.bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')),
        "NEEDLETAIL_BUILD_GIT may hold letters, digits, '.', '_' and '-' only: {full:?}"
    );
    let dirty = git(&["status", "--porcelain"]).is_some_and(|status| !status.is_empty());
    let unix = std::env::var("SOURCE_DATE_EPOCH")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |elapsed| elapsed.as_secs())
        });
    let short: String = full.chars().take(12).collect();
    let id = if dirty {
        format!("{short}-dirty")
    } else {
        short
    };
    let package = std::env::var("CARGO_PKG_VERSION").unwrap_or_default();
    println!("cargo:rustc-env=AV_MESH_BUILD_GIT_FULL={full}");
    println!("cargo:rustc-env=AV_MESH_BUILD_DIRTY={dirty}");
    println!("cargo:rustc-env=AV_MESH_BUILD_UNIX={unix}");
    println!("cargo:rustc-env=AV_MESH_BUILD_GIT={id}");
    println!("cargo:rustc-env=AV_MESH_BUILD_VERSION={package} ({id} {unix})");
}
