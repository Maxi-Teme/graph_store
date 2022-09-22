use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;

use actix::{Actor, Context, Handler};

use petgraph::stable_graph::{Neighbors, NodeIndex, StableGraph};
use petgraph::visit::{IntoEdgeReferences, IntoNodeReferences};
use petgraph::Directed;
use petgraph::Direction::{Incoming, Outgoing};

use crate::graph_store::GraphStore;
use crate::{
    GraphEdge, GraphMutation, GraphNode, GraphNodeIndex, GraphQuery,
    GraphResponse,
};

use super::StoreError;

#[derive(Debug, Clone, Default)]
pub struct Graph<N, E, I>
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static,
{
    inner: StableGraph<N, E, Directed>,
    nodes_map: HashMap<I, NodeIndex>,
    store: GraphStore<N, E>,
}

impl<N, E, I> Actor for Graph<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Context = Context<Self>;
}

impl<N, E, I> Handler<GraphMutation<N, E, I>> for Graph<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Result = Result<GraphResponse<N, E, I>, StoreError>;

    fn handle(
        &mut self,
        msg: GraphMutation<N, E, I>,
        _: &mut Self::Context,
    ) -> Self::Result {
        match msg {
            GraphMutation::AddEdge((from, to, edge)) => {
                self.add_edge(from, to, edge)?;
                Ok(GraphResponse::Empty)
            }
            GraphMutation::RemoveEdge((from, to)) => {
                let edge = self.remove_edge(from, to)?;
                Ok(GraphResponse::Edge(edge))
            }
            GraphMutation::AddNode((key, node)) => {
                let node = self.add_node(key, node)?;
                Ok(GraphResponse::Node(node))
            }
            GraphMutation::RemoveNode(key) => {
                let node = self.remove_node(key)?;
                Ok(GraphResponse::Node(node))
            }
        }
    }
}

impl<N, E, I> Handler<GraphQuery<N, E, I>> for Graph<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Result = Result<GraphResponse<N, E, I>, StoreError>;

    fn handle(
        &mut self,
        msg: GraphQuery<N, E, I>,
        _: &mut Self::Context,
    ) -> Self::Result {
        match msg {
            GraphQuery::GetGraph => {
                let graph = self.get_graph()?;
                Ok(GraphResponse::Graph(graph))
            }
            GraphQuery::FilterGraph((include_nodes, include_edges)) => {
                let graph = self.filter_graph(include_nodes, include_edges)?;
                Ok(GraphResponse::Graph(graph))
            }
            GraphQuery::RetainNodes(nodes_to_retain) => {
                let graph = self.retain_nodes(nodes_to_retain)?;
                Ok(GraphResponse::Graph(graph))
            }
            GraphQuery::GetNeighbors(key) => {
                let nodes = self.get_neighbors(&key)?;
                Ok(GraphResponse::Nodes(nodes))
            }
            GraphQuery::GetEdge((from, to)) => {
                let edge = self.get_edge(&from, &to)?;
                Ok(GraphResponse::Edge(edge))
            }
            GraphQuery::GetEdges => {
                let edges = self.get_edges()?;
                Ok(GraphResponse::Edges(edges))
            }
            GraphQuery::HasNode(key) => {
                let has = self.has_node(&key)?;
                Ok(GraphResponse::Bool(has))
            }
            GraphQuery::GetNode(key) => {
                let node = self.get_node(&key)?;
                Ok(GraphResponse::Node(node))
            }
            GraphQuery::GetNodes => {
                let nodes = self.get_nodes()?;
                Ok(GraphResponse::Nodes(nodes))
            }
            GraphQuery::GetNodeIndex(key) => {
                let node_index = self.get_node_index(&key)?;
                Ok(GraphResponse::NodeIndex(node_index))
            }
            GraphQuery::GetSourceNodes => {
                let nodes = self.get_source_nodes()?;
                Ok(GraphResponse::Nodes(nodes))
            }
            GraphQuery::GetSinkNodes => {
                let nodes = self.get_sink_nodes()?;
                Ok(GraphResponse::Nodes(nodes))
            }
        }
    }
}

impl<N, E, I> Graph<N, E, I>
where
    N: GraphNode,
    E: GraphEdge,
    I: GraphNodeIndex + From<N>,
{
    // constructors
    //

    pub fn new(store_path: Option<String>) -> Result<Self, StoreError> {
        let store = GraphStore::new(store_path)?;
        let inner = store.load_from_file()?;
        let nodes_map = Self::get_nodes_map_from_graph(&inner);

        log::info!("Initialized graph");

        Ok(Self {
            inner,
            nodes_map,
            store,
        })
    }

    //
    // public interface
    //

    fn add_edge(&mut self, from: I, to: I, edge: E) -> Result<(), StoreError> {
        if let Ok(_) = self.get_edge(&from, &to) {
            return Err(StoreError::ConflictDuplicateEdge);
        };

        let from_idx = self.get_node_index(&from)?;
        let to_idx = self.get_node_index(&to)?;

        let mut graph = self.inner.clone();

        graph.add_edge(from_idx, to_idx, edge);

        self.inner = self.store.save_to_file(graph)?;

        Ok(())
    }

    fn remove_edge(&mut self, from: I, to: I) -> Result<E, StoreError> {
        if let (Some(from_idx), Some(to_idx)) =
            (self.nodes_map.get(&from), self.nodes_map.get(&to))
        {
            let mut graph = self.inner.clone();

            let edge_index = self
                .inner
                .find_edge(*from_idx, *to_idx)
                .ok_or(StoreError::EdgeNotFound)?;

            let edge = graph
                .remove_edge(edge_index)
                .ok_or(StoreError::EdgeNotFound)?;

            self.inner = self.store.save_to_file(graph)?;

            Ok(edge)
        } else {
            Err(StoreError::EdgeNotFound)
        }
    }

    fn add_node(&mut self, key: I, node: N) -> Result<N, StoreError> {
        match self.nodes_map.entry(key) {
            Entry::Vacant(vacant_entry) => {
                let mut graph = self.inner.clone();

                let new_node_index = graph.add_node(node.clone());

                self.inner = self.store.save_to_file(graph)?;
                vacant_entry.insert(new_node_index);

                Ok(node)
            }
            Entry::Occupied(_) => Err(StoreError::ConflictDuplicateNode),
        }
    }

    fn remove_node(&mut self, key: I) -> Result<N, StoreError> {
        if let Entry::Occupied(map_entry) = self.nodes_map.entry(key) {
            let mut graph = self.inner.clone();

            let removed_node = graph
                .remove_node(*map_entry.get())
                .ok_or(StoreError::NodeNotDeleted)?;

            self.inner = self.store.save_to_file(graph)?;
            map_entry.remove();

            Ok(removed_node)
        } else {
            Err(StoreError::NodeNotFound)
        }
    }

    fn get_graph(&self) -> Result<StableGraph<N, E, Directed>, StoreError> {
        Ok(self.inner.clone())
    }

    fn filter_graph(
        &self,
        include_nodes: Option<Vec<N>>,
        include_edges: Option<Vec<E>>,
    ) -> Result<StableGraph<N, E, Directed>, StoreError> {
        let filtered = match (include_nodes, include_edges) {
            (Some(nodes), Some(edges)) => {
                let mut nodes = nodes.into_iter();
                let node_filter = |_, node: &N| {
                    nodes.find(|n| {
                        log::info!("n: {:?} == node: {:?}", n, node);
                        n == node
                    })
                };
                let mut edges = edges.into_iter();
                let edge_filter = |_, edge: &E| {
                    edges.find(|e| {
                        log::info!("e: {:?} == edge: {:?}", e, edge);
                        e == edge
                    })
                };

                self.inner.filter_map(node_filter, edge_filter)
            }
            (Some(nodes), None) => {
                let mut nodes = nodes.into_iter();
                let node_filter = |_, node: &N| nodes.find(|n| n == node);
                let edge_filter = |_, edge: &E| Some((*edge).clone());

                log::info!("(Some(nodes), None)");

                self.inner.filter_map(node_filter, edge_filter)
            }
            (None, Some(edges)) => {
                let node_filter = |_, node: &N| Some((*node).clone());
                let mut edges = edges.into_iter();
                let edge_filter = |_, edge: &E| edges.find(|e| e == edge);

                self.inner.filter_map(node_filter, edge_filter)
            }
            (None, None) => self.inner.clone(),
        };

        Ok(filtered)
    }

    fn retain_nodes(
        &self,
        nodes_to_retain: Vec<I>,
    ) -> Result<StableGraph<N, E, Directed>, StoreError> {
        let mut retained = self.inner.clone(); // could become expensive

        let node_indices: Vec<&NodeIndex> =
            self.get_node_indices(nodes_to_retain)?.clone();

        retained.retain_nodes(|_, nidx| node_indices.contains(&&nidx));

        Ok(retained)
    }

    fn get_neighbors(&self, key: &I) -> Result<Vec<N>, StoreError> {
        let neighbors_idxs = self.get_neighbor_indices(key)?;

        let neighbors = neighbors_idxs
            .filter_map(|n| self.inner.node_weight(n))
            .cloned()
            .collect();

        Ok(neighbors)
    }

    fn get_edge(&self, from: &I, to: &I) -> Result<E, StoreError> {
        if let (Some(from_idx), Some(to_idx)) =
            (self.nodes_map.get(from), self.nodes_map.get(to))
        {
            let edge_index = self
                .inner
                .find_edge(*from_idx, *to_idx)
                .ok_or(StoreError::EdgeNotFound)?;

            self.inner
                .edge_weight(edge_index)
                .cloned()
                .ok_or(StoreError::EdgeNotFound)
        } else {
            Err(StoreError::EdgeNotFound)
        }
    }

    fn get_edges(&self) -> Result<Vec<E>, StoreError> {
        let edge_references = self.inner.edge_references();

        let edges = edge_references.map(|e| e.weight()).cloned().collect();

        Ok(edges)
    }

    fn has_node(&self, key: &I) -> Result<bool, StoreError> {
        Ok(self.nodes_map.contains_key(key))
    }

    fn get_node(&self, key: &I) -> Result<N, StoreError> {
        let node_index = self.get_node_index(key)?;

        let node = self
            .inner
            .node_weight(node_index)
            .ok_or(StoreError::NodeNotFound)?;

        Ok(node.clone())
    }

    fn get_nodes(&self) -> Result<Vec<N>, StoreError> {
        let node_references = self.inner.node_references();

        let nodes = node_references.map(|(_, n)| n).cloned().collect();

        Ok(nodes)
    }

    fn get_node_index(&self, key: &I) -> Result<NodeIndex, StoreError> {
        let node_index =
            self.nodes_map.get(key).ok_or(StoreError::NodeNotFound)?;

        Ok(*node_index)
    }

    fn get_node_indices(
        &self,
        keys: Vec<I>,
    ) -> Result<Vec<&NodeIndex>, StoreError> {
        Ok(keys.iter().filter_map(|k| self.nodes_map.get(k)).collect())
    }

    fn get_source_nodes(&self) -> Result<Vec<N>, StoreError> {
        let externals = self.inner.externals(Incoming);
        let externals = externals
            .filter_map(|n| self.inner.node_weight(n))
            .cloned()
            .collect();

        Ok(externals)
    }

    fn get_sink_nodes(&self) -> Result<Vec<N>, StoreError> {
        let externals = self.inner.externals(Outgoing);
        let externals = externals
            .filter_map(|n| self.inner.node_weight(n))
            .cloned()
            .collect();

        Ok(externals)
    }

    //
    // private methods
    //

    fn get_nodes_map_from_graph(
        data: &StableGraph<N, E, Directed>,
    ) -> HashMap<I, NodeIndex<u32>> {
        let mut nodes_map = HashMap::new();

        for (index, node) in data.node_references() {
            nodes_map.insert(node.clone().into(), index);
        }

        nodes_map
    }

    fn get_neighbor_indices(
        &self,
        key: &I,
    ) -> Result<Neighbors<E>, StoreError> {
        let found_node_index =
            self.nodes_map.get(key).ok_or(StoreError::NodeNotFound)?;

        Ok(self.inner.neighbors(*found_node_index))
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    use super::*;

    #[derive(
        Debug,
        Default,
        Clone,
        Copy,
        Hash,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Serialize,
        Deserialize,
    )]
    struct SimpleEdgeType(usize);
    impl GraphEdge for SimpleEdgeType {}

    #[derive(
        Debug,
        Default,
        Clone,
        Copy,
        Hash,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Serialize,
        Deserialize,
    )]
    struct SimpleNodeType {
        id: Uuid,
    }
    impl GraphNode for SimpleNodeType {}

    #[derive(
        Debug,
        Default,
        Clone,
        Copy,
        Hash,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Serialize,
        Deserialize,
    )]
    struct SimpleNodeWeightIndex(Uuid);

    impl GraphNodeIndex for SimpleNodeWeightIndex {}

    impl From<SimpleNodeType> for SimpleNodeWeightIndex {
        fn from(node: SimpleNodeType) -> Self {
            Self(node.id)
        }
    }

    #[tokio::test]
    async fn test_store_from_env_ok() {
        let test_dir = "test-data/test_store_from_env_ok";

        if std::path::Path::new(test_dir).is_dir() {
            std::fs::remove_dir_all(test_dir).unwrap();
        }

        let mut graph = Graph::<
            SimpleNodeType,
            SimpleEdgeType,
            SimpleNodeWeightIndex,
        >::new(Some(test_dir.into()))
        .unwrap();

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let node1 = SimpleNodeType { id: id1 };
        let node2 = SimpleNodeType { id: id2 };

        graph.add_node(SimpleNodeWeightIndex(id1), node1).unwrap();
        graph.add_node(SimpleNodeWeightIndex(id2), node2).unwrap();

        let nodes = graph.get_nodes().unwrap();
        assert_eq!(nodes.len(), 2);

        if std::path::Path::new(test_dir).is_dir() {
            std::fs::remove_dir_all(test_dir).unwrap();
        }
    }

    #[tokio::test]
    async fn test_store_add_node_duplicate_key_nok() {
        let test_dir = "test-data/test_store_add_node_duplicate_key_nok";
        if std::path::Path::new(test_dir).is_dir() {
            std::fs::remove_dir_all(test_dir).unwrap();
        }

        let mut graph = Graph::<
            SimpleNodeType,
            SimpleEdgeType,
            SimpleNodeWeightIndex,
        >::new(Some(test_dir.into()))
        .unwrap();

        let id = Uuid::new_v4();
        let node = SimpleNodeType { id };

        graph.add_node(SimpleNodeWeightIndex(id), node).unwrap();

        assert_eq!(
            graph.add_node(SimpleNodeWeightIndex(id), node),
            Err(StoreError::ConflictDuplicateNode)
        );

        if std::path::Path::new(test_dir).is_dir() {
            std::fs::remove_dir_all(test_dir).unwrap();
        }
    }

    #[tokio::test]
    async fn test_store_add_node_duplicate_combined_key_nok() {
        let test_dir = "test-data/duplicate_combined_key";
        if std::path::Path::new(test_dir).is_dir() {
            std::fs::remove_dir_all(test_dir).unwrap();
        }

        #[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
        struct CombinedNodeType {
            label: String,
            id: Uuid,
        }
        impl GraphNode for CombinedNodeType {}

        #[derive(
            Debug,
            Clone,
            Default,
            Hash,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Serialize,
            Deserialize,
        )]
        struct CombinedNodeWeightIndex((String, Uuid));

        impl GraphNodeIndex for CombinedNodeWeightIndex {}

        impl From<CombinedNodeType> for CombinedNodeWeightIndex {
            fn from(node: CombinedNodeType) -> Self {
                Self((node.label, node.id))
            }
        }

        let mut graph = Graph::<
            CombinedNodeType,
            SimpleEdgeType,
            CombinedNodeWeightIndex,
        >::new(Some(test_dir.into()))
        .unwrap();

        let label = String::from("some label");
        let id = Uuid::new_v4();
        let node = CombinedNodeType { label, id };

        graph
            .add_node(
                CombinedNodeWeightIndex((String::from("some label"), id)),
                node,
            )
            .unwrap();

        let result = graph.add_node(
            CombinedNodeWeightIndex((String::from("some label"), id)),
            CombinedNodeType {
                label: String::from("some label"),
                id,
            },
        );

        assert_eq!(result, Err(StoreError::ConflictDuplicateNode));

        if std::path::Path::new(test_dir).is_dir() {
            std::fs::remove_dir_all(test_dir).unwrap();
        }
    }
}
