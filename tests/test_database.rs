mod common;

#[tokio::test]
async fn test_database_initialization() {
    let (database, teardown) =
        common::setup("test-data/test_database_initialization", 4000).await;

    let nodes = database.get_nodes().await.unwrap();

    assert_eq!(nodes.len(), 0);

    teardown();
}
