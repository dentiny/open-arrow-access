use super::valid_relative_file_path;

#[test]
fn preserves_logical_path_characters_and_interior_whitespace() {
    for path in [
        "paper.pdf",
        "papers/论文.pdf",
        "papers/paper #1%.pdf",
        "dir/ file.txt",
        "dir /file.txt",
        "a?b:c",
        ".hidden/...",
    ] {
        assert!(valid_relative_file_path(path), "{path:?}");
    }
}

#[test]
fn rejects_noncanonical_paths_controls_and_edge_whitespace() {
    for path in [
        "",
        "/paper.pdf",
        "paper.pdf/",
        "a//b",
        ".",
        "..",
        "a/./b",
        "a/../b",
        "a\\b",
        "a\0b",
        "a\nb",
        "a\u{85}b",
        " paper.pdf",
        "paper.pdf\u{2003}",
    ] {
        assert!(!valid_relative_file_path(path), "{path:?}");
    }
}
