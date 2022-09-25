use uuid::Uuid;

use common::{Edge, Node};

mod common;

#[actix_rt::test]
async fn test_basic_single_process() {
    env_logger::init();

    let test_dir = "test-data/test_basic_single_process";

    let database = common::setup(test_dir, 4000, vec![]).await;

    let node1_id = Uuid::new_v4();
    let node1 = Node::new(node1_id);

    let node2_id = Uuid::new_v4();
    let node2 = Node::new(node2_id);

    database.add_node(node1).await.unwrap();
    database.add_node(node2).await.unwrap();
    database.add_edge(node1, node2, Edge(1)).await.unwrap();

    let resulting_graph = database.get_graph().await.unwrap();
    let edges: Vec<_> = resulting_graph.edge_indices().into_iter().collect();
    let nodes: Vec<_> = resulting_graph.node_indices().into_iter().collect();
    assert_eq!(edges.len(), 1);
    assert_eq!(nodes.len(), 2);

    database.remove_node(node1).await.unwrap();
    database.remove_node(node2).await.unwrap();

    let resulting_graph = database.get_graph().await.unwrap();
    let edges: Vec<_> = resulting_graph.edge_indices().into_iter().collect();
    let nodes: Vec<_> = resulting_graph.node_indices().into_iter().collect();
    assert_eq!(edges.len(), 0);
    assert_eq!(nodes.len(), 0);

    let node3_id = Uuid::new_v4();
    let node3 = Node::new(node3_id);

    let node4_id = Uuid::new_v4();
    let node4 = Node::new(node4_id);

    database.add_node(node1).await.unwrap();
    database.add_node(node2).await.unwrap();
    database.add_node(node3).await.unwrap();
    database.add_node(node4).await.unwrap();

    let resulting_graph = database.get_graph().await.unwrap();
    let edges: Vec<_> = resulting_graph.edge_indices().into_iter().collect();
    let nodes: Vec<_> = resulting_graph.node_indices().into_iter().collect();
    assert_eq!(edges.len(), 0);
    assert_eq!(nodes.len(), 4);

    let retained_nodes = database
        .retain_nodes(vec![node1.into(), node3.into()])
        .await
        .unwrap();

    let edges: Vec<_> = retained_nodes.edge_indices().into_iter().collect();
    let nodes: Vec<_> = retained_nodes.node_indices().into_iter().collect();
    assert_eq!(edges.len(), 0);
    assert_eq!(nodes.len(), 2);

    database.add_edge(node1, node3, Edge(14)).await.unwrap();

    let retained_nodes = database
        .retain_nodes(vec![node1.into(), node3.into()])
        .await
        .unwrap();

    let edges: Vec<_> = retained_nodes.edge_indices().into_iter().collect();
    let nodes: Vec<_> = retained_nodes.node_indices().into_iter().collect();
    assert_eq!(edges.len(), 1);
    assert_eq!(nodes.len(), 2);

    let retained_nodes = database
        .retain_nodes(vec![node1.into(), node2.into()])
        .await
        .unwrap();

    let edges: Vec<_> = retained_nodes.edge_indices().into_iter().collect();
    let nodes: Vec<_> = retained_nodes.node_indices().into_iter().collect();
    assert_eq!(edges.len(), 0);
    assert_eq!(nodes.len(), 2);

    database.remove_node(node1).await.unwrap();
    database.remove_node(node2).await.unwrap();
    database.remove_node(node3).await.unwrap();
    database.remove_node(node4).await.unwrap();
    database.add_node(node1).await.unwrap();
    database.add_node(node2).await.unwrap();
    database.add_node(node3).await.unwrap();
    database.add_node(node4).await.unwrap();
    database.add_edge(node1, node3, Edge(14)).await.unwrap();

    let retained_nodes = database
        .retain_nodes(vec![node1.into(), node3.into()])
        .await
        .unwrap();

    let edges: Vec<_> = retained_nodes.edge_indices().into_iter().collect();
    let nodes: Vec<_> = retained_nodes.node_indices().into_iter().collect();
    assert_eq!(edges.len(), 1);
    assert_eq!(nodes.len(), 2);

    if std::path::Path::new(test_dir).is_dir() {
        std::fs::remove_dir_all(test_dir).unwrap();
    }
}
