use std::path::PathBuf;

use walkdir::{DirEntry, WalkDir};

use crate::codex_proto as p;

const MATCH_LIMIT: usize = 50;
const ENTRY_LIMIT: usize = 100_000;

pub async fn handle_fuzzy_file_search(
    params: p::FuzzyFileSearchParams,
) -> p::FuzzyFileSearchResponse {
    if params.query.trim().is_empty() || params.roots.is_empty() {
        return p::FuzzyFileSearchResponse { files: Vec::new() };
    }

    let query = params.query;
    let roots = params.roots;
    let files = tokio::task::spawn_blocking(move || search(&query, &roots))
        .await
        .unwrap_or_default();
    p::FuzzyFileSearchResponse { files }
}

fn search(query: &str, roots: &[String]) -> Vec<p::FuzzyFileSearchResult> {
    let mut matches = Vec::new();
    let mut visited = 0usize;

    for root in roots {
        let root_path = PathBuf::from(root);
        if !root_path.is_dir() {
            continue;
        }
        for entry in WalkDir::new(&root_path)
            .follow_links(false)
            .into_iter()
            .filter_entry(searchable_entry)
            .filter_map(Result::ok)
            .skip(1)
        {
            if visited >= ENTRY_LIMIT {
                break;
            }
            visited += 1;

            let relative = entry
                .path()
                .strip_prefix(&root_path)
                .unwrap_or(entry.path());
            let candidate = relative.to_string_lossy();
            let Some((score, indices)) = fuzzy_match(query, &candidate) else {
                continue;
            };
            matches.push(p::FuzzyFileSearchResult {
                root: root.clone(),
                path: entry.path().to_string_lossy().into_owned(),
                match_type: if entry.file_type().is_dir() {
                    p::FuzzyFileSearchMatchType::Directory
                } else {
                    p::FuzzyFileSearchMatchType::File
                },
                file_name: entry.file_name().to_string_lossy().into_owned(),
                score,
                indices: Some(indices),
            });
        }
        if visited >= ENTRY_LIMIT {
            break;
        }
    }

    matches.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.path.cmp(&right.path))
    });
    matches.truncate(MATCH_LIMIT);
    matches
}

fn searchable_entry(entry: &DirEntry) -> bool {
    if entry.depth() == 0 || !entry.file_type().is_dir() {
        return true;
    }
    !matches!(
        entry.file_name().to_str(),
        Some(".git" | ".hg" | ".svn" | "node_modules" | "target" | ".build")
    )
}

fn fuzzy_match(query: &str, candidate: &str) -> Option<(u32, Vec<u32>)> {
    let query: Vec<char> = query.chars().map(|ch| ch.to_ascii_lowercase()).collect();
    if query.is_empty() {
        return None;
    }
    let candidate_chars: Vec<char> = candidate.chars().collect();
    let candidate_lower: Vec<char> = candidate_chars
        .iter()
        .map(|ch| ch.to_ascii_lowercase())
        .collect();

    let file_name_start = candidate_chars
        .iter()
        .rposition(|ch| std::path::is_separator(*ch))
        .map_or(0, |index| index + 1);
    candidate_lower
        .iter()
        .enumerate()
        .filter(|(_, candidate)| **candidate == query[0])
        .filter_map(|(first, _)| {
            let mut indices = vec![first as u32];
            let mut cursor = first + 1;
            for needle in query.iter().skip(1) {
                let offset = candidate_lower[cursor..]
                    .iter()
                    .position(|candidate| candidate == needle)?;
                let index = cursor + offset;
                indices.push(index as u32);
                cursor = index + 1;
            }
            let last = *indices.last()? as usize;
            let gaps = last + 1 - first - indices.len();
            let contiguous_bonus = if gaps == 0 { 500 } else { 0 };
            let file_name_bonus = if first >= file_name_start { 250 } else { 0 };
            let prefix_bonus = if first == file_name_start { 250 } else { 0 };
            let penalty = gaps
                .saturating_mul(8)
                .saturating_add(candidate_chars.len().saturating_sub(indices.len()));
            Some((
                10_000u32
                    .saturating_add(contiguous_bonus)
                    .saturating_add(file_name_bonus)
                    .saturating_add(prefix_bonus)
                    .saturating_sub(penalty as u32),
                indices,
            ))
        })
        .max_by_key(|(score, _)| *score)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fuzzy_match_prefers_file_name_prefix_and_returns_indices() {
        let prefix = fuzzy_match("read", "src/readme.md").unwrap();
        let embedded = fuzzy_match("read", "src/bread.md").unwrap();
        assert!(prefix.0 > embedded.0);
        assert_eq!(prefix.1, vec![4, 5, 6, 7]);
    }

    #[test]
    fn search_returns_files_and_directories_in_score_order() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(temp.path().join("src")).unwrap();
        std::fs::write(temp.path().join("src/readme.md"), "hello").unwrap();
        std::fs::write(temp.path().join("main.rs"), "hello").unwrap();

        let root = temp.path().to_string_lossy().into_owned();
        let results = search("read", std::slice::from_ref(&root));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].file_name, "readme.md");
        assert_eq!(results[0].root, root);
        assert_eq!(results[0].match_type, p::FuzzyFileSearchMatchType::File);
    }

    #[test]
    fn search_skips_dependency_and_vcs_directories() {
        let temp = tempfile::tempdir().unwrap();
        for directory in [".git", "node_modules", "target"] {
            std::fs::create_dir_all(temp.path().join(directory)).unwrap();
            std::fs::write(temp.path().join(directory).join("needle.txt"), "hidden").unwrap();
        }
        std::fs::write(temp.path().join("needle.txt"), "visible").unwrap();

        let root = temp.path().to_string_lossy().into_owned();
        let results = search("needle", &[root]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].file_name, "needle.txt");
    }

    #[test]
    fn empty_query_and_missing_roots_return_no_results() {
        assert!(search("", &["/definitely/missing".into()]).is_empty());
        assert!(search("file", &["/definitely/missing".into()]).is_empty());
    }
}
