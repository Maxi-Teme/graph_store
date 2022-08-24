use actix::{Actor, AsyncContext, Context, Handler, WrapFuture};

use futures_util::TryFutureExt;
use tonic::transport::Channel;
use tonic::Request;

use crate::sync_graph::sync_graph_client::SyncGraphClient;
use crate::sync_graph::{
    AddEdgeRequest, AddNodeRequest, RemoveEdgeRequest, RemoveNodeRequest,
};
use crate::{
    GraphEdge, GraphNode, GraphNodeIndex, MutatinGraphQuery, StoreError,
};

#[derive(Debug, Clone)]
pub struct GraphClient {
    // addr: String,
    client: SyncGraphClient<Channel>,
}

impl Actor for GraphClient {
    type Context = Context<Self>;
}

impl GraphClient {
    pub async fn new(address: String) -> Result<Self, StoreError> {
        let client = SyncGraphClient::connect(address.clone())
            .map_err(|err| {
                log::warn!(
                    "Could not connect initial client to '{}'. Error: '{}'",
                    address,
                    err
                );
                StoreError::ClientError
            })
            .await?;

        Ok(Self { client })
    }
}

impl<N, E, I> Handler<MutatinGraphQuery<N, E, I>> for GraphClient
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Result = Result<(), StoreError>;

    fn handle(
        &mut self,
        msg: MutatinGraphQuery<N, E, I>,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        let mut client = self.client.clone();

        let future = Box::pin(async move {
            match msg {
                MutatinGraphQuery::AddEdge((from, to, edge)) => {
                    let (from, to, edge) = match (
                        bincode::serialize(&from),
                        bincode::serialize(&to),
                        bincode::serialize(&edge),
                    ) {
                        (Ok(from), Ok(to), Ok(edge)) => (from, to, edge),
                        _ => return log::error!(
                            "Error while serializing 'AddEdgeRequest' query."
                        ),
                    };

                    let request =
                        Request::new(AddEdgeRequest { from, to, edge });
                    if let Err(err) = client.add_edge(request).await {
                        log::error!(
                            "Error while sending 'AddEdgeRequest' request. Error: {err}"
                        );
                    };
                }
                MutatinGraphQuery::RemoveEdge((from, to)) => {
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
                MutatinGraphQuery::AddNode((key, node)) => {
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
                MutatinGraphQuery::RemoveNode(key) => {
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
        Ok(())
    }
}
