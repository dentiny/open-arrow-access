use super::valid_filename_component;

#[test]
fn preserves_logical_filename_characters() {
    for value in [
        "paper.pdf",
        "paper #1%.pdf",
        "a?b:c",
        ".hidden",
        "...",
        "论文.pdf",
        "first\u{a0}second.pdf",
    ] {
        assert!(valid_filename_component(value), "{value:?}");
    }
}

#[test]
fn rejects_empty_dot_separators_controls_and_edge_whitespace() {
    for value in [
        "",
        ".",
        "..",
        "a/b",
        "a\\b",
        "/paper.pdf",
        "paper.pdf/",
        "a\0b",
        "a\nb",
        "a\u{7f}b",
        "a\u{85}b",
        " paper.pdf",
        "paper.pdf ",
        "\u{a0}paper.pdf",
        "paper.pdf\u{2003}",
    ] {
        assert!(!valid_filename_component(value), "{value:?}");
    }
}
