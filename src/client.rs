use std::collections::HashMap;
use std::error::Error;
use std::marker::PhantomData;

use actix::{
    Actor, AsyncContext, Context, Handler, ResponseActFuture, WrapFuture,
};

use futures_util::TryFutureExt;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

use crate::sync_graph::sync_graph_client::SyncGraphClient;
use crate::sync_graph::{
    AddEdgeRequest, AddNodeRequest, RemotesLogRequest, RemoveEdgeRequest,
    RemoveNodeRequest,
};
use crate::{
    GraphEdge, GraphMutation, GraphNode, GraphNodeIndex, GraphResponse,
    StoreError, SyncRemotesMessage,
};

#[derive(Debug, Clone)]
pub struct GraphClient<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    client: SyncGraphClient<Channel>,
    phantom_n: PhantomData<N>,
    phantom_e: PhantomData<E>,
    phantom_i: PhantomData<I>,
}

impl<N, E, I> Actor for GraphClient<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Context = Context<Self>;
}

impl<N, E, I> GraphClient<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    pub async fn new(address: String) -> Result<Self, StoreError> {
        let endpoint = Endpoint::try_from(address)
            .map_err(|_err| StoreError::ParseError)?;

        let client = SyncGraphClient::connect(endpoint.clone())
            .map_err(|err| {
                log::warn!(
                    "Could not connect client at {:?}. Error: '{}' source: '{:?}'",
                    endpoint.uri(),
                    err,
                    err.source(),
                );
                StoreError::ClientError
            })
            .await?;

        Ok(Self {
            client,
            phantom_n: PhantomData,
            phantom_e: PhantomData,
            phantom_i: PhantomData,
        })
    }
}

impl<N, E, I> Handler<SyncRemotesMessage> for GraphClient<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Result =
        ResponseActFuture<Self, Result<HashMap<String, bool>, StoreError>>;

    fn handle(
        &mut self,
        msg: SyncRemotesMessage,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let SyncRemotesMessage {
            from,
            flat_remotes_log,
        } = msg;

        let request = Request::new(RemotesLogRequest {
            from_server: from,
            remotes_log: flat_remotes_log,
        });

        let mut client = self.client.clone();

        let future = Box::pin(async move {
            match client.sync_remotes(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    Ok(response.remotes_log)
                }
                Err(err) => {
                    log::error!(
                        "Error sending add_remote request. Error: '{err}'"
                    );
                    Err(StoreError::ClientError)
                }
            }
        });

        let actor_future = future.into_actor(self);

        Box::pin(actor_future)
    }
}

impl<N, E, I> Handler<GraphMutation<N, E, I>> for GraphClient<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Result = Result<GraphResponse<N, E, I>, StoreError>;

    fn handle(
        &mut self,
        msg: GraphMutation<N, E, I>,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        let mut client = self.client.clone();

        let future = Box::pin(async move {
            match msg {
                GraphMutation::AddEdge((from, to, edge)) => {
                    let (from, to, edge) = match (
                        bincode::serialize(&from),
                        bincode::serialize(&to),
                        bincode::serialize(&edge),
                    ) {
                        (Ok(from), Ok(to), Ok(edge)) => (from, to, edge),
                        _ => {
                            return log::error!(
                            "Error while serializing 'AddEdgeRequest' query."
                        )
                        }
                    };

                    let request =
                        Request::new(AddEdgeRequest { from, to, edge });
                    if let Err(err) = client.add_edge(request).await {
                        log::error!(
                            "Error while sending 'AddEdgeRequest' request. Error: {err}"
                        );
                    };
                }
                GraphMutation::RemoveEdge((from, to)) => {
                    let (from, to) = match (
                        bincode::serialize(&from),
                        bincode::serialize(&to),
                    ) {
                        (Ok(from), Ok(to)) => (from, to),
                        _ => {
                            return log::error!(
                                "Error while serializing 'RemoveEdge' query."
                            )
                        }
                    };

                    let request = Request::new(RemoveEdgeRequest { from, to });
                    if let Err(err) = client.remove_edge(request).await {
                        log::error!("Error while sending 'RemoveEdge' request. Error: {err}");
                    };
                }
                GraphMutation::AddNode((key, node)) => {
                    let (key, node) = match (
                        bincode::serialize(&key),
                        bincode::serialize(&node),
                    ) {
                        (Ok(key), Ok(node)) => (key, node),
                        _ => {
                            return log::error!(
                                "Error while serializing 'AddNode' query."
                            )
                        }
                    };

                    let request = Request::new(AddNodeRequest { key, node });
                    if let Err(err) = client.add_node(request).await {
                        log::error!(
                            "Error while sending 'AddNode' request. Error: {err}"
                        );
                    };
                }
                GraphMutation::RemoveNode(key) => {
                    let key = match bincode::serialize(&key) {
                        Ok(key) => key,
                        Err(err) => {
                            return log::error!(
                                "Error while serializing 'AddNode' query. Error: {err}"
                            )
                        },
                    };

                    let request = Request::new(RemoveNodeRequest { key });
                    if let Err(err) = client.remove_node(request).await {
                        log::error!("Error while sending 'remove_node' request. Error: {err}");
                    };
                }
            }
        });

        let actor_future = future.into_actor(self);
        ctx.spawn(actor_future);

        Ok(GraphResponse::Empty)
    }
}
