use std::collections::HashMap;
use std::marker::PhantomData;

use actix::{
    Actor, ActorFutureExt, Addr, Context, Handler, Message, ResponseActFuture,
    WrapFuture,
};

use actix_interop::FutureInterop;

use crate::graph::Graph;
use crate::mutations_log_store::{MutationsLogStore, MutationsLogStoreMessage};
use crate::remotes::Remotes;
use crate::{
    GraphEdge, GraphMutation, GraphNode, GraphNodeIndex, GraphResponse,
    SendToNMessage, StoreError,
};

pub struct MutationsLog<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    graph: Addr<Graph<N, E, I>>,
    remotes: Addr<Remotes<N, E, I>>,
    mutations_log_store: Addr<MutationsLogStore<N, E, I>>,
    pending_mutations_log: HashMap<String, GraphMutation<N, E, I>>,
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
        let pending_mutations_log = HashMap::new();

        Ok(Self {
            graph,
            remotes,
            mutations_log_store,
            pending_mutations_log,
        })
    }

    pub async fn commit_mutation(
        mutations_log_store_addr: &Addr<MutationsLogStore<N, E, I>>,
        graph_addr: &Addr<Graph<N, E, I>>,
        graph_mutation: GraphMutation<N, E, I>,
    ) -> Result<GraphResponse<N, E, I>, StoreError> {
        mutations_log_store_addr
            .send(MutationsLogStoreMessage::Commit(graph_mutation.clone()))
            .await??;

        graph_addr.send(graph_mutation).await?
    }
}

pub enum LogMessage<N, E, I>
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static,
{
    Replicated(GraphMutation<N, E, I>),
    Commit(GraphMutation<N, E, I>),
}

impl<N, E, I> Message for LogMessage<N, E, I>
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static,
{
    type Result = Result<GraphResponse<N, E, I>, StoreError>;
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

        async move {
            match msg {
                LogMessage::Replicated(graph_mutation) => {
                    mutations_log_store
                        .send(MutationsLogStoreMessage::Add(
                            graph_mutation.clone(),
                        ))
                        .await??;

                    // includes also remotes to which we send
                    // synchronous mutations
                    remotes.do_send(graph_mutation.clone());

                    remotes
                        .send(SendToNMessage(graph_mutation.clone()))
                        .await??;

                    Self::commit_mutation(
                        &mutations_log_store,
                        &graph,
                        graph_mutation,
                    )
                    .await
                }
                LogMessage::Commit(graph_mutation) => {
                    Self::commit_mutation(
                        &mutations_log_store,
                        &graph,
                        graph_mutation,
                    )
                    .await
                }
            }
        }
        .interop_actor_boxed(self)
    }
}

pub struct MutationsLogQuery<N, E, I> {
    phantom_n: PhantomData<N>,
    phantom_e: PhantomData<E>,
    phantom_i: PhantomData<I>,
}

impl<N, E, I> MutationsLogQuery<N, E, I> {
    pub const INST: MutationsLogQuery<N, E, I> = MutationsLogQuery {
        phantom_n: PhantomData,
        phantom_e: PhantomData,
        phantom_i: PhantomData,
    };
}

impl<N, E, I> Message for MutationsLogQuery<N, E, I>
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static,
{
    type Result = Result<HashMap<String, GraphMutation<N, E, I>>, StoreError>;
}

impl<N, E, I> Handler<MutationsLogQuery<N, E, I>> for MutationsLog<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Result = ResponseActFuture<
        Self,
        Result<HashMap<String, GraphMutation<N, E, I>>, StoreError>,
    >;

    fn handle(
        &mut self,
        msg: MutationsLogQuery<N, E, I>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let mutations_log_store = self.mutations_log_store.clone();

        async move { mutations_log_store.send(msg).await? }
            .interop_actor_boxed(self)
    }
}

pub struct InitializeMutationsLog;

impl Message for InitializeMutationsLog {
    type Result = Result<(), StoreError>;
}

impl<N, E, I> Handler<InitializeMutationsLog> for MutationsLog<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Result = ResponseActFuture<Self, Result<(), StoreError>>;

    fn handle(
        &mut self,
        _msg: InitializeMutationsLog,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let remotes = self.remotes.clone();
        let graph = self.graph.clone();
        let mutations_log_store = self.mutations_log_store.clone();
        let mut pending_mutations_log = self.pending_mutations_log.clone();

        let future = async move {
            let mutations_log = remotes.send(MutationsLogQuery::INST).await??;

            for (hash, graph_mutation) in mutations_log.into_iter() {
                if let Err(err) = Self::commit_mutation(
                    &mutations_log_store,
                    &graph,
                    graph_mutation.clone(),
                )
                .await
                {
                    log::warn!(
                        "Error committing GraphMutations \
while synchronizing. Error: '{:?}'",
                        err
                    );

                    pending_mutations_log.insert(hash, graph_mutation);
                }
            }

            Ok(pending_mutations_log)
        };

        let actor_future = future.into_actor(self).map(
            |pending_mutations: Result<
                HashMap<std::string::String, GraphMutation<N, E, I>>,
                StoreError,
            >,
             actor,
             _ctx| {
                actor.pending_mutations_log = pending_mutations?;
                Ok(())
            },
        );

        Box::pin(actor_future)
    }
}
