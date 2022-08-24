use std::fmt::Debug;
use std::net::SocketAddr;

use actix::Addr;

use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

use crate::graph::Graph;
use crate::sync_graph::sync_graph_server::{SyncGraph, SyncGraphServer};
use crate::sync_graph::{
    AddEdgeRequest, AddNodeRequest, ProcessResponse, RemoveEdgeRequest,
    RemoveNodeRequest, ResponseType,
};
use crate::{
    GraphEdge, GraphNode, GraphNodeIndex, GraphQuery, MutatinGraphQuery,
    StoreError,
};

#[derive(Debug)]
pub struct GraphServer<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    graph_addr: Addr<Graph<N, E, I>>,
}

impl<N, E, I> GraphServer<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    pub async fn run(
        graph_addr: Addr<Graph<N, E, I>>,
        server_address: SocketAddr,
    ) -> Result<(), StoreError> {
        let sync_graph_server = Self { graph_addr };

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

        Ok(())
    }
}

#[tonic::async_trait]
impl<N, E, I> SyncGraph for GraphServer<N, E, I>
where
    N: GraphNode + Unpin,
    E: GraphEdge + Unpin,
    I: GraphNodeIndex + From<N> + Unpin,
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

        log::debug!(
            "Got add_edge request. from: '{:?}' to: '{:?}' edge: '{:?}'",
            from,
            to,
            edge
        );

        let query = MutatinGraphQuery::AddEdge((from, to, edge));

        self.graph_addr.do_send(GraphQuery::Mutating(query));

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

        let query = MutatinGraphQuery::RemoveEdge((from, to));
        self.graph_addr.do_send(GraphQuery::Mutating(query));

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

        let query = MutatinGraphQuery::AddNode((key, node));
        self.graph_addr.do_send(GraphQuery::Mutating(query));

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

        let query = MutatinGraphQuery::RemoveNode(key);
        self.graph_addr.do_send(GraphQuery::Mutating(query));

        Ok(Response::new(ProcessResponse {
            response_type: ResponseType::Ok.into(),
        }))
    }
}
