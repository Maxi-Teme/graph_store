use common::{SimpleNodeType, SimpleNodeWeightIndex};
use uuid::Uuid;

mod common;

const TOTAL_NODES: usize = 1;

#[actix_rt::test]
async fn test_basic_two_processes() {
    let root_test_path = "test-data/test_basic_two_processes";

    let test_dir1 = "test-data/test_basic_two_processes/node1";
    let database1 = common::setup(test_dir1, 4000, vec![]).await;

    let (thread2_tx, thread2_rx) = tokio::sync::oneshot::channel();
    let thread2 = actix_rt::spawn(async move {
        let test_dir2 = "test-data/test_basic_two_processes/node2";
        let initial_remotes2 = vec!["http://127.0.0.1:4000".to_string()];
        let database2 = common::setup(test_dir2, 4001, initial_remotes2).await;

        thread2_tx.send(true).unwrap();

        let mut nodes2: Vec<_> = Vec::new();
        let mut count = 0;

        while nodes2.len() < TOTAL_NODES {
            if count > 100 {
                return Err("Polling exceeded");
            }

            nodes2 = database2.get_nodes().await.unwrap();
            count += 1;
        }

        Ok(nodes2)
    });

    thread2_rx.await.unwrap();

    let first_node_id = Uuid::new_v4();
    database1
        .add_node(
            SimpleNodeWeightIndex(first_node_id.clone()),
            SimpleNodeType::new(first_node_id.clone()),
        )
        .await
        .unwrap();

    let nodes1 = database1.get_nodes().await.unwrap();
    let nodes2 = thread2.await.unwrap().unwrap();

    assert_eq!(nodes1.len(), TOTAL_NODES);
    assert_eq!(nodes2.len(), TOTAL_NODES);
    assert_eq!(nodes1, nodes2);

    database1.remove_node(SimpleNodeWeightIndex(first_node_id.clone())).await.unwrap();
    let nodes1 = database1.get_nodes().await.unwrap();
    assert_eq!(nodes1.len(), TOTAL_NODES - 1);

    database1
        .add_node(
            SimpleNodeWeightIndex(first_node_id.clone()),
            SimpleNodeType::new(first_node_id.clone()),
        )
        .await
        .unwrap();
    let nodes1 = database1.get_nodes().await.unwrap();
    assert_eq!(nodes1.len(), TOTAL_NODES);

    if std::path::Path::new(root_test_path).is_dir() {
        std::fs::remove_dir_all(root_test_path).unwrap();
    }
}
