use actix::{Actor, Addr, Context, Handler, Message, ResponseActFuture};

use actix_interop::FutureInterop;

use crate::graph::Graph;
use crate::mutations_log_store::{MutationsLogStore, MutationsLogStoreMessage};
use crate::remotes::Remotes;
use crate::{
    GraphEdge, GraphMutation, GraphNode, GraphNodeIndex, GraphResponse,
    SendToNMessage, StoreError,
};

pub enum LogMessage<N, E, I>
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static,
{
    Replicated(GraphMutation<N, E, I>),
    Single(GraphMutation<N, E, I>),
}

impl<N, E, I> Message for LogMessage<N, E, I>
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static,
{
    type Result = Result<GraphResponse<N, E, I>, StoreError>;
}

pub struct MutationsLog<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    graph: Addr<Graph<N, E, I>>,
    remotes: Addr<Remotes<N, E, I>>,
    mutations_log_store: Addr<MutationsLogStore<N, E, I>>,
}

impl<N, E, I> Actor for MutationsLog<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Context = Context<Self>;
}

impl<N, E, I> MutationsLog<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    //
    // constuctor

    pub async fn new(
        graph: Addr<Graph<N, E, I>>,
        remotes: Addr<Remotes<N, E, I>>,
        store_path: Option<String>,
    ) -> Result<Self, StoreError> {
        let mutations_log_store = MutationsLogStore::new(store_path).start();

        Ok(Self {
            graph,
            remotes,
            mutations_log_store,
        })
    }
}

impl<N, E, I> Handler<LogMessage<N, E, I>> for MutationsLog<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Result =
        ResponseActFuture<Self, Result<GraphResponse<N, E, I>, StoreError>>;

    fn handle(
        &mut self,
        msg: LogMessage<N, E, I>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let remotes = self.remotes.clone();
        let graph = self.graph.clone();
        let mutations_log_store = self.mutations_log_store.clone();

        match msg {
            LogMessage::Replicated(graph_mutation) => async move {
                mutations_log_store
                    .send(MutationsLogStoreMessage::Add(graph_mutation.clone()))
                    .await??;

                remotes.do_send(graph_mutation.clone());

                remotes
                    .send(SendToNMessage(graph_mutation.clone()))
                    .await??;

                mutations_log_store
                    .send(MutationsLogStoreMessage::Commit(
                        graph_mutation.clone(),
                    ))
                    .await??;

                graph.send(graph_mutation.clone()).await?
            }
            .interop_actor_boxed(self),
            LogMessage::Single(graph_mutation) => async move {
                mutations_log_store
                    .send(MutationsLogStoreMessage::Commit(
                        graph_mutation.clone(),
                    ))
                    .await??;

                graph.send(graph_mutation.clone()).await?
            }
            .interop_actor_boxed(self),
        }
    }
}
