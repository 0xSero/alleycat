use std::path::Path;
use std::process::Command;

use alleycat_codex_proto::GitInfo;

/// Render a page without re-running Git for every thread in the same directory.
/// The cache lasts only for this response; the next request sees fresh metadata.
/// Git subprocesses run off the async executor so they cannot stall other RPCs.
pub async fn map_entries_with_git_info<M, T, F>(
    entries: Vec<crate::IndexEntry<M>>,
    mut convert: F,
) -> anyhow::Result<Vec<T>>
where
    M: Send + 'static,
    T: Send + 'static,
    F: FnMut(&crate::IndexEntry<M>, Option<GitInfo>) -> T + Send + 'static,
{
    Ok(tokio::task::spawn_blocking(move || {
        let mut by_cwd = std::collections::HashMap::new();
        entries
            .iter()
            .map(|entry| {
                let info = by_cwd
                    .entry(&entry.cwd)
                    .or_insert_with(|| git_info_for_cwd(&entry.cwd));
                convert(entry, info.clone())
            })
            .collect()
    })
    .await?)
}

/// Best-effort Git metadata for a thread cwd.
///
/// Codex derives this from the working directory when listing threads. Bridges
/// already know each thread's cwd, so mirror the same surface without persisting
/// it into bridge indexes. Missing git, non-repo paths, detached heads, and
/// repos without an origin simply leave individual fields empty.
pub fn git_info_for_cwd(cwd: impl AsRef<Path>) -> Option<GitInfo> {
    let cwd = cwd.as_ref();
    if cwd.as_os_str().is_empty() || !cwd.is_dir() {
        return None;
    }

    let sha = git_output(cwd, &["rev-parse", "--verify", "HEAD"]);
    let branch = git_output(cwd, &["branch", "--show-current"]);
    let origin_url = git_output(cwd, &["config", "--get", "remote.origin.url"]);

    if sha.is_none() && branch.is_none() && origin_url.is_none() {
        return None;
    }

    Some(GitInfo {
        sha,
        branch,
        origin_url,
    })
}

fn git_output(cwd: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(cwd)
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    let value = text.lines().next().unwrap_or("").trim();
    if value.is_empty() || value == "HEAD" {
        None
    } else {
        Some(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(cwd: &Path, id: usize) -> crate::IndexEntry<()> {
        serde_json::from_value(serde_json::json!({
            "threadId": id.to_string(), "cwd": cwd.to_string_lossy(),
            "createdAt": 0, "updatedAt": 0, "preview": "", "modelProvider": "test",
            "source": "appServer"
        }))
        .unwrap()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn page_lookup_is_fresh_between_requests_and_runs_off_executor() {
        let repo = tempfile::tempdir().unwrap();
        let plain = tempfile::tempdir().unwrap();
        assert!(run_git(repo.path(), &["init", "-b", "main"]));
        assert!(run_git(
            repo.path(),
            &["remote", "add", "origin", "https://example.com/old.git"]
        ));
        let entries = vec![
            entry(repo.path(), 0),
            entry(repo.path(), 1),
            entry(plain.path(), 2),
            entry(plain.path(), 3),
        ];
        let executor = std::thread::current().id();
        let page = map_entries_with_git_info(entries.clone(), move |entry, info| {
            assert_ne!(std::thread::current().id(), executor);
            // Changes during projection must not trigger repeat lookups in a page.
            if entry.thread_id == "0" {
                assert!(run_git(
                    Path::new(&entry.cwd),
                    &["remote", "set-url", "origin", "https://example.com/new.git"]
                ));
            } else if entry.thread_id == "2" {
                assert!(run_git(Path::new(&entry.cwd), &["init", "-b", "new-repo"]));
            }
            (entry.thread_id.clone(), info)
        })
        .await
        .unwrap();
        assert_eq!(
            page.iter().map(|row| row.0.as_str()).collect::<Vec<_>>(),
            ["0", "1", "2", "3"]
        );
        for (_, info) in &page[..2] {
            assert_eq!(
                info.as_ref().unwrap().origin_url.as_deref(),
                Some("https://example.com/old.git")
            );
        }
        assert!(page[2].1.is_none() && page[3].1.is_none());
        let refreshed = map_entries_with_git_info(entries, |_, info| info)
            .await
            .unwrap();
        assert_eq!(
            refreshed[0].as_ref().unwrap().origin_url.as_deref(),
            Some("https://example.com/new.git")
        );
        assert_eq!(
            refreshed[2].as_ref().unwrap().branch.as_deref(),
            Some("new-repo")
        );
    }

    #[tokio::test]
    #[ignore = "manual subprocess latency measurement"]
    async fn profile_repeated_directory_page() {
        let repo = tempfile::tempdir().unwrap();
        assert!(run_git(repo.path(), &["init", "-b", "main"]));
        let entries: Vec<_> = (0..25).map(|id| entry(repo.path(), id)).collect();
        for iteration in 0..5 {
            let start = std::time::Instant::now();
            let baseline: Vec<_> = entries
                .iter()
                .map(|entry| git_info_for_cwd(&entry.cwd))
                .collect();
            let baseline_ms = start.elapsed().as_secs_f64() * 1000.0;
            let start = std::time::Instant::now();
            let actual = map_entries_with_git_info(entries.clone(), |_, info| info)
                .await
                .unwrap();
            let candidate_ms = start.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(actual, baseline);
            eprintln!(
                "iteration={iteration} baseline_ms={baseline_ms:.3} candidate_ms={candidate_ms:.3} threads=25 distinct_cwds=1"
            );
        }
    }

    fn run_git(cwd: &Path, args: &[&str]) -> bool {
        Command::new("git")
            .arg("-C")
            .arg(cwd)
            .args(args)
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    fn git_available() -> bool {
        Command::new("git")
            .arg("--version")
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    #[test]
    fn returns_none_for_non_repo() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(git_info_for_cwd(dir.path()), None);
    }

    #[test]
    fn reads_git_metadata_from_cwd() {
        if !git_available() {
            return;
        }

        let dir = tempfile::tempdir().unwrap();
        if !run_git(dir.path(), &["init", "-b", "main"]) {
            assert!(run_git(dir.path(), &["init"]));
        }
        assert!(run_git(
            dir.path(),
            &["config", "user.email", "test@example.com"]
        ));
        assert!(run_git(dir.path(), &["config", "user.name", "Test User"]));
        assert!(run_git(
            dir.path(),
            &["remote", "add", "origin", "https://example.com/repo.git"]
        ));
        std::fs::write(dir.path().join("README.md"), "hello\n").unwrap();
        assert!(run_git(dir.path(), &["add", "README.md"]));
        assert!(run_git(dir.path(), &["commit", "-m", "init"]));

        let info = git_info_for_cwd(dir.path()).expect("expected git metadata");
        assert!(info.sha.as_deref().is_some_and(|sha| sha.len() == 40));
        assert!(
            info.branch
                .as_deref()
                .is_some_and(|branch| !branch.is_empty())
        );
        assert_eq!(
            info.origin_url.as_deref(),
            Some("https://example.com/repo.git")
        );
    }
}
