mod filesystem {
    use arrow_array::types::Int64Type;
    use arrow_schema::{DataType, Field, Schema};
    use futures::TryStreamExt;
    use open_arrow_access_core::{
        Accessor, ByteAccessor, ErrorKind, ListRequest, ObjectRef, RecordReader, into_stream,
    };
    use std::{num::NonZeroUsize, sync::Arc};

    fn schema(label: &str) -> Arc<Schema> {
        Arc::new(Schema::new_with_metadata(
            vec![Field::new("id", DataType::Int64, false)],
            [("label".into(), label.into())].into(),
        ))
    }

    async fn values(reader: Box<dyn RecordReader>) -> Vec<i64> {
        open_arrow_access_test_support::record::collect_column::<Int64Type>(reader, 0).await
    }
    use more_asserts::assert_le;
    use open_arrow_access_dataset::directory::{DirectoryAccessor, DirectoryRequest};
    use open_arrow_access_file_format::parquet::ParquetAccessor;
    use open_arrow_access_storage::OpenDalAccessor;
    fn copy_fixture(name: &str, destination: &std::path::Path) {
        let source = open_arrow_access_test_support::fixture_path!("parquet/directory", name);
        std::fs::copy(source, destination).unwrap();
    }

    #[tokio::test]
    async fn filesystem_listing_is_recursive_file_only_and_uses_backend_prefix_semantics() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        copy_fixture("a.parquet", &dir.path().join("a"));
        copy_fixture("b.parquet", &dir.path().join("sub/b"));
        let fs = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(dir.path()));
        assert!(fs.capability().list);
        let mut paths = fs
            .list("".into())
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        paths.sort_by(|a, b| a.locator().cmp(b.locator()));
        assert_eq!(paths, vec![ObjectRef::new("a"), ObjectRef::new("sub/b")]);
        assert_eq!(
            fs.list(ListRequest::new("").with_recursive(false))
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            vec![ObjectRef::new("a")]
        );
        assert_eq!(
            fs.list("sub".into())
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            vec![ObjectRef::new("sub/b")]
        );
        // OpenDAL Fs treats nonexistent and non-directory prefixes as empty.
        for prefix in ["missing", "a"] {
            assert!(
                fs.list(prefix.into())
                    .await
                    .unwrap()
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn nonrecursive_directory_does_not_enter_unreadable_subdirectories() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        copy_fixture("a.parquet", &dir.path().join("a.parquet"));
        let excluded = dir.path().join("excluded");
        std::fs::create_dir(&excluded).unwrap();
        copy_fixture("b.parquet", &excluded.join("b.parquet"));
        let permissions = std::fs::metadata(&excluded).unwrap().permissions();
        let fs = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(dir.path()));
        let accessor = DirectoryAccessor::new(fs.clone(), ParquetAccessor::new(fs));
        let mut request = DirectoryRequest::new("");
        request.recursive = false;
        request.suffix = Some(".parquet".into());
        std::fs::set_permissions(&excluded, std::fs::Permissions::from_mode(0o0)).unwrap();
        let result = accessor.open(request).await;
        // Restore permissions before assertions so failed reads still clean up.
        std::fs::set_permissions(&excluded, permissions).unwrap();
        // This also checks selection when tests run as root and bypass permissions.
        assert_eq!(values(result.unwrap()).await, [1, 2]);
    }

    #[tokio::test]
    async fn directory_reads_local_parquet_in_sorted_order_with_projection_and_byte_options() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(".sub")).unwrap();
        let schema = schema("same");
        copy_fixture("b.parquet", &dir.path().join("b.parquet"));
        copy_fixture("a.parquet", &dir.path().join("a.parquet"));
        copy_fixture("c.parquet", &dir.path().join(".sub/c.parquet"));
        copy_fixture("hidden.parquet", &dir.path().join(".hidden.parquet"));
        std::fs::copy(
            open_arrow_access_test_support::fixture_path!("csv", "good.csv"),
            dir.path().join("readme"),
        )
        .unwrap();
        let fs = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(dir.path()));
        let accessor = DirectoryAccessor::new(fs.clone(), ParquetAccessor::new(fs));
        let mut request = DirectoryRequest::new("");
        request.suffix = Some(".parquet".into());
        request.options.batch_size = NonZeroUsize::new(1).unwrap();
        request.byte_options.io_concurrency = NonZeroUsize::new(2).unwrap();
        request.byte_options.io_chunk_size = NonZeroUsize::new(8);
        let reader = accessor.open(request.clone()).await.unwrap();
        assert_eq!(reader.metadata().schema, schema);
        assert_eq!(values(reader).await, [99, 4, 1, 2, 3]);
        request.options.projection = Some(vec![]);
        let batches = into_stream(accessor.open(request.clone()).await.unwrap())
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 5);
        for batch in &batches {
            assert_eq!(batch.num_columns(), 0);
            assert_le!(batch.num_rows(), 1);
        }
        request.recursive = false;
        request.options.projection = None;
        assert_eq!(
            values(accessor.open(request.clone()).await.unwrap()).await,
            [99, 1, 2, 3]
        );
        request.suffix = Some(".csv".into());
        assert_eq!(
            accessor.open(request).await.err().unwrap().kind(),
            ErrorKind::InvalidInput
        );
    }

    #[tokio::test]
    async fn directory_flattens_parquet_row_groups_without_changing_file_order() {
        use futures::StreamExt;
        use open_arrow_access_core::{AccessorExt, ReadOptions, ScanPlanner};
        use open_arrow_access_layers::ParallelReadLayer;

        let dir = tempfile::tempdir().unwrap();
        for name in ["a.parquet", "b.parquet"] {
            std::fs::copy(
                open_arrow_access_test_support::fixture_path!("parquet", "ordered-snappy.parquet"),
                dir.path().join(name),
            )
            .unwrap();
        }
        let storage = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(dir.path()));
        let directory = DirectoryAccessor::new(storage.clone(), ParquetAccessor::new(storage))
            .with_file_planning();
        let mut request = DirectoryRequest::new("");
        request.options = ReadOptions {
            projection: Some(vec!["id".into()]),
            ..Default::default()
        };
        let mut plan = directory.plan_scan(request.clone()).await.unwrap();
        let mut partitions = 0;
        while plan.partitions.next().await.transpose().unwrap().is_some() {
            partitions += 1;
        }
        assert_eq!(partitions, 8);
        let reader = directory
            .layer(ParallelReadLayer::new(
                NonZeroUsize::new(3).unwrap(),
                NonZeroUsize::MIN,
            ))
            .open(request)
            .await
            .unwrap();
        assert_eq!(
            values(reader).await,
            [0, 1, 2, 3, 4, 5, 6, 0, 1, 2, 3, 4, 5, 6]
        );
    }
}
