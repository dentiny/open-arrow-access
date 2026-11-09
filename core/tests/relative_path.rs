use open_arrow_access_core::is_relative_path;

#[test]
fn checks_components_without_imposing_provider_character_rules() {
    for path in [
        "file",
        "dir/file",
        ".hidden/...",
        "dir/ file",
        "dir /file",
        "论文",
        "a?b#c",
        "a\\b",
        "a\0b",
    ] {
        assert!(is_relative_path(path), "{path:?}");
    }
    for path in [
        "", ".", "..", "/", "/file", "file/", "a//b", "a/./b", "a/../b",
    ] {
        assert!(!is_relative_path(path), "{path:?}");
    }
}
