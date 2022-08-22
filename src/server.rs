use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

use crate::graph::Graph;
use crate::sync_graph::sync_graph_server::{SyncGraph, SyncGraphServer};
use crate::sync_graph::{
    AddEdgeRequest, AddNodeRequest, ProcessResponse, RemoveEdgeRequest,
    RemoveNodeRequest, ResponseType,
};
use crate::{GraphEdge, GraphNode, GraphNodeIndex, MutateGraph};

#[derive(Debug, Default)]
pub struct GraphServer<N, E, I>
where
    N: GraphNode,
    E: GraphEdge,
    I: GraphNodeIndex + From<N>,
{
    graph: Arc<RwLock<Graph<N, E, I>>>,
}

impl<N, E, I> GraphServer<N, E, I>
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static,
{
    pub async fn new(
        graph: Arc<RwLock<Graph<N, E, I>>>,
        server_address: SocketAddr,
    ) -> Self {
        let sync_graph_server = Self::default();

        tokio::spawn(async move {
            match Server::builder()
                .add_service(SyncGraphServer::new(sync_graph_server))
                .serve(server_address)
                .await
            {
                Ok(()) => log::debug!("Initialized server"),
                Err(err) => {
                    panic!("Error while starting gRPC server. Error: '{err}'")
                }
            }
        });

        Self { graph }
    }
}

#[tonic::async_trait]
impl<N, E, I> SyncGraph for GraphServer<N, E, I>
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static,
{
    async fn add_edge(
        &self,
        request: Request<AddEdgeRequest>,
    ) -> Result<Response<ProcessResponse>, Status> {
        let AddEdgeRequest { from, to, edge } = request.into_inner();

        let (from, to, edge) = match (
            bincode::deserialize(&from),
            bincode::deserialize(&to),
            bincode::deserialize(&edge),
        ) {
            (Ok(from), Ok(to), Ok(edge)) => (from, to, edge),
            _ => return Err(Status::new(Code::Internal, "")),
        };

        if let Ok(mut graph) = self.graph.write() {
            if let Err(err) = graph.add_edge(&from, &to, edge) {
                return Err(Status::new(Code::Internal, &format!("{err:?}")));
            }
        }

        Ok(Response::new(ProcessResponse {
            response_type: ResponseType::Ok.into(),
        }))
    }

    async fn remove_edge(
        &self,
        request: Request<RemoveEdgeRequest>,
    ) -> Result<Response<ProcessResponse>, Status> {
        let RemoveEdgeRequest { from, to } = request.into_inner();

        let (from, to) =
            match (bincode::deserialize(&from), bincode::deserialize(&to)) {
                (Ok(from), Ok(to)) => (from, to),
                _ => return Err(Status::new(Code::Internal, "")),
            };

        if let Ok(mut graph) = self.graph.write() {
            if let Err(err) = graph.remove_edge(&from, &to) {
                return Err(Status::new(Code::Internal, &format!("{err:?}")));
            }
        }

        Ok(Response::new(ProcessResponse {
            response_type: ResponseType::Ok.into(),
        }))
    }

    async fn add_node(
        &self,
        request: Request<AddNodeRequest>,
    ) -> Result<Response<ProcessResponse>, Status> {
        let AddNodeRequest { key, node } = request.into_inner();

        let (key, node) =
            match (bincode::deserialize(&key), bincode::deserialize(&node)) {
                (Ok(key), Ok(node)) => (key, node),
                _ => return Err(Status::new(Code::Internal, "")),
            };

        if let Ok(mut graph) = self.graph.write() {
            if let Err(err) = graph.add_node(key, node) {
                return Err(Status::new(Code::Internal, &format!("{err:?}")));
            }
        }

        Ok(Response::new(ProcessResponse {
            response_type: ResponseType::Ok.into(),
        }))
    }

    async fn remove_node(
        &self,
        request: Request<RemoveNodeRequest>,
    ) -> Result<Response<ProcessResponse>, Status> {
        let RemoveNodeRequest { key } = request.into_inner();

        let key = match bincode::deserialize(&key) {
            Ok(key) => key,
            Err(err) => {
                return Err(Status::new(Code::Internal, &format!("{err:?}")))
            }
        };

        if let Ok(mut graph) = self.graph.write() {
            if let Err(err) = graph.remove_node(key) {
                return Err(Status::new(Code::Internal, &format!("{err:?}")));
            };
        }

        Ok(Response::new(ProcessResponse {
            response_type: ResponseType::Ok.into(),
        }))
    }
}
