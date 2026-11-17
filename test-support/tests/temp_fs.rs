use open_arrow_access_test_support::temp_fs;

#[tokio::test]
async fn nested_files_preserve_bytes_and_root_guard_controls_cleanup() {
    let (root, operator) = temp_fs(&[("nested/data", &[0, 255, 10]), ("empty", &[])]);
    let path = root.path().to_path_buf();
    assert_eq!(
        operator.read("nested/data").await.unwrap().to_vec(),
        [0, 255, 10]
    );
    assert!(operator.read("empty").await.unwrap().is_empty());
    drop(operator);
    assert!(path.is_dir());
    drop(root);
    assert!(!path.exists());
}

#[test]
#[should_panic(expected = "temporary fixture path must stay within the root")]
fn parent_paths_are_rejected() {
    temp_fs(&[("../outside", &[])]);
}

#[test]
#[should_panic(expected = "temporary fixture path must stay within the root")]
fn absolute_paths_are_rejected() {
    temp_fs(&[("/outside", &[])]);
}
