use common::{SimpleNodeType, SimpleNodeWeightIndex};
use uuid::Uuid;

mod common;

const TOTAL_NODES: usize = 3;

#[actix_rt::test]
async fn test_initial_data_synchronization() {
    env_logger::init();

    let root_test_path = "test-data/test_two_nodes_ok";

    let test_dir1 = "test-data/test_two_nodes_ok/node1";
    let database1 = common::setup(test_dir1, 4000, vec![]).await;

    let first_node_id = Uuid::new_v4();
    database1
        .add_node(
            SimpleNodeWeightIndex(first_node_id.clone()),
            SimpleNodeType::new(first_node_id.clone()),
        )
        .await
        .unwrap();

    let second_node_id = Uuid::new_v4();
    database1
        .add_node(
            SimpleNodeWeightIndex(second_node_id.clone()),
            SimpleNodeType::new(second_node_id.clone()),
        )
        .await
        .unwrap();

    let (thread2_tx, thread2_rx) = tokio::sync::oneshot::channel();
    let thread2 = actix_rt::spawn(async move {
        let test_dir2 = "test-data/test_two_nodes_ok/node2";
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

    let third_node_id = Uuid::new_v4();
    database1
        .add_node(
            SimpleNodeWeightIndex(third_node_id.clone()),
            SimpleNodeType::new(third_node_id.clone()),
        )
        .await
        .unwrap();

    let nodes1 = database1.get_nodes().await.unwrap();
    let nodes2 = thread2.await.unwrap().unwrap();

    assert_eq!(nodes1.len(), TOTAL_NODES);
    assert_eq!(nodes2.len(), TOTAL_NODES);

    for node in nodes1 {
        assert!(nodes2.contains(&node))
    }

    if std::path::Path::new(root_test_path).is_dir() {
        std::fs::remove_dir_all(root_test_path).unwrap();
    }
}
