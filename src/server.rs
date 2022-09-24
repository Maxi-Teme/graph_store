use std::fmt::Debug;
use std::net::SocketAddr;

use actix::Addr;

use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::mutations_log::{
    MutationsLog, MutationsLogMutation, MutationsLogQuery,
};
use crate::remotes::Remotes;
use crate::sync_graph::sync_graph_server::{SyncGraph, SyncGraphServer};
use crate::sync_graph::{
    GraphMutationRequest, GraphMutationResponse, MutationsLogRequest,
    MutationsLogResponse, RemotesLogRequest, RemotesLogResponse,
};
use crate::{
    GraphEdge, GraphNode, GraphNodeIndex, StoreError, SyncRemotesMessage,
};

#[derive(Debug)]
pub struct GraphServer<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    server_address: String,
    mutations_log: Addr<MutationsLog<N, E, I>>,
    remotes: Addr<Remotes<N, E, I>>,
}

impl<N, E, I> GraphServer<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    pub fn run(
        server_address: SocketAddr,
        mutations_log: Addr<MutationsLog<N, E, I>>,
        remotes: Addr<Remotes<N, E, I>>,
    ) -> Result<(), StoreError> {
        let sync_graph_server = Self {
            server_address: server_address.clone().to_string(),
            mutations_log,
            remotes,
        };
        let service = SyncGraphServer::new(sync_graph_server);

        tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve(server_address)
                .await
                .unwrap()
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
    async fn sync_remotes(
        &self,
        request: Request<RemotesLogRequest>,
    ) -> Result<Response<RemotesLogResponse>, Status> {
        let RemotesLogRequest {
            from_server,
            remotes_log,
        } = request.into_inner();

        // pass RemotesLogRequest to remotes
        match self
            .remotes
            .send(SyncRemotesMessage {
                from: from_server.clone(),
                flat_remotes_log: remotes_log,
            })
            .await
            .map_err(|err| Status::internal(format!("{}", err)))?
        {
            // respond with own flat_remotes_log
            Ok(flat_remotes_log) => Ok(Response::new(RemotesLogResponse {
                from_server: self.server_address.clone(),
                remotes_log: flat_remotes_log,
            })),
            Err(err) => {
                let msg = format!(
                    "SyncRemotesMessage({:?}) failed. Error: '{:?}'",
                    self.server_address, err
                );
                Err(Status::internal(msg))
            }
        }
    }

    async fn sync_mutations_log(
        &self,
        _request: Request<MutationsLogRequest>,
    ) -> Result<Response<MutationsLogResponse>, Status> {
        match self.mutations_log.send(MutationsLogQuery::full()).await {
            Ok(inner) => match inner {
                Ok(mutations_log) => {
                    let mutations_log: Vec<u8> =
                        bincode::serialize(&mutations_log)
                            .map_err(|err| Status::internal(err.to_string()))?;

                    Ok(Response::new(MutationsLogResponse { mutations_log }))
                }
                Err(err) => {
                    log::error!(
                        "Error while getting mutations log. Error: {:?}",
                        err
                    );
                    Err(Status::internal(err.to_string()))
                }
            },
            Err(err) => {
                log::error!(
                    "Error while sending MutationsLogQuery. Error: {}",
                    err
                );
                Err(Status::internal(err.to_string()))
            }
        }
    }

    async fn graph_mutation(
        &self,
        request: Request<GraphMutationRequest>,
    ) -> Result<Response<GraphMutationResponse>, Status> {
        let graph_mutation_log_entry: MutationsLogMutation<N, E, I> = request
            .into_inner()
            .try_into()
            .map_err(|err: StoreError| Status::internal(err.to_string()))?;

        match self.mutations_log.send(graph_mutation_log_entry).await {
            Ok(inner) => match inner {
                Ok(_) => Ok(Response::new(GraphMutationResponse {})),
                Err(err) => {
                    log::error!(
                        "Error while committing to log. Error: {:?}",
                        err
                    );
                    Err(Status::internal(""))
                }
            },
            Err(err) => {
                log::error!(
                    "Error while sending log commit to MutationsLog. Error: {}",
                    err
                );
                Err(Status::internal(""))
            }
        }
    }
}
