use std::collections::HashMap;

use actix::{
    Actor, ActorFutureExt, Addr, Context, Handler, Message, ResponseActFuture,
    WrapFuture,
};
use actix_interop::FutureInterop;
use rand::seq::SliceRandom;

use crate::mutations_log::{MutationsLogMutation, MutationsLogQuery};
use crate::{GraphClient, GraphEdge, GraphNode, GraphNodeIndex, StoreError};

pub struct SyncRemotesMessage {
    pub from: String,
    pub flat_remotes_log: HashMap<String, bool>,
}

impl Message for SyncRemotesMessage {
    type Result = Result<HashMap<String, bool>, StoreError>;
}

#[derive(Debug, Clone)]
struct RemotesEntry<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    client_addr: Addr<GraphClient<N, E, I>>,
    remotes_log: Result<HashMap<String, bool>, StoreError>,
}

#[derive(Debug, Clone)]
pub struct Remotes<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    remotes: HashMap<String, RemotesEntry<N, E, I>>,
    sync_with_n: usize,
}

impl<N, E, I> Remotes<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    pub fn new(sync_with_n: usize) -> Self {
        Self {
            remotes: HashMap::new(),
            sync_with_n,
        }
    }
}

impl<N, E, I> Actor for Remotes<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Context = Context<Self>;
}

impl<N, E, I> Handler<MutationsLogMutation<N, E, I>> for Remotes<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    // type Result = Result<(), StoreError>;
    type Result = ResponseActFuture<Self, Result<(), StoreError>>;

    fn handle(
        &mut self,
        msg: MutationsLogMutation<N, E, I>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let sync_with_n = self.sync_with_n.clone();
        let mut rng = rand::thread_rng();
        let random_remotes: Vec<Addr<_>> = self
            .remotes
            .clone()
            .into_iter()
            .filter_map(|(_, remotes_entry)| {
                remotes_entry
                    .remotes_log
                    .ok()
                    .map(|_| remotes_entry.client_addr)
            })
            .collect();

        let future = Box::pin(async move {
            let random_remotes =
                random_remotes.choose_multiple(&mut rng, sync_with_n.into());

            for addr in random_remotes {
                addr.send(msg.clone()).await??;
            }

            Ok(())
        });
        let actor_future = future.into_actor(self);

        Box::pin(actor_future)
    }
}

impl<N, E, I> Handler<SyncRemotesMessage> for Remotes<N, E, I>
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
        _: &mut Self::Context,
    ) -> Self::Result {
        let SyncRemotesMessage {
            from,
            flat_remotes_log,
        } = msg;

        let mut remotes = self.remotes.clone();

        let future = async move {
            let from = from.clone();

            // check if our remotes has entry already, then insert or update
            if let Some(entry) = remotes.get(&from) {
                let mut entry = entry.clone();
                entry.remotes_log = Ok(flat_remotes_log.clone());
                remotes.insert(from.clone(), entry);
            } else if let Ok(client) = GraphClient::new(from.clone()).await {
                let client_addr = client.start();

                let remotes_entry = RemotesEntry {
                    client_addr,
                    remotes_log: Ok(flat_remotes_log),
                };

                remotes.insert(from, remotes_entry);
            };

            Ok(remotes)
        };

        let actor_future = future.into_actor(self).map(
            |result: Result<HashMap<String, RemotesEntry<N, E, I>>, E>,
             actor,
             _ctx| match result {
                Ok(remotes_log) => {
                    actor.remotes = remotes_log.clone();

                    let mut flat_remotes_log: HashMap<String, bool> =
                        HashMap::new();

                    for (uri, entry) in remotes_log.into_iter() {
                        flat_remotes_log.insert(uri, entry.remotes_log.is_ok());
                    }

                    Ok(flat_remotes_log)
                }
                Err(err) => {
                    log::error!(
                        "Error resolving add_remote future. Error: '{:?}'",
                        err
                    );
                    Err(StoreError::ClientError)
                }
            },
        );

        Box::pin(actor_future)
    }
}

pub struct InitializeRemotes {
    pub initial_addresses: Vec<String>,
    pub server_address: String,
}

impl Message for InitializeRemotes {
    type Result = Result<(), StoreError>;
}

impl<N, E, I> Handler<InitializeRemotes> for Remotes<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Result = ResponseActFuture<Self, Result<(), StoreError>>;

    fn handle(
        &mut self,
        msg: InitializeRemotes,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let initial_addresses = msg.initial_addresses;
        let server_address = msg.server_address;
        let mut remote_addresses: HashMap<String, Addr<_>> = HashMap::new();
        let mut initial_flat_remotes_log: HashMap<String, bool> =
            HashMap::new();

        let future = Box::pin(async move {
            // build up initial clients
            for address in initial_addresses {
                if let Ok(client) = GraphClient::new(address.clone()).await {
                    let client_addr = client.start();

                    remote_addresses.insert(address.clone(), client_addr);
                    initial_flat_remotes_log.insert(address, true);
                }
            }

            let mut remotes: HashMap<String, RemotesEntry<N, E, I>> =
                HashMap::new();

            // send RemotesLogRequest to all known remotes
            for (remote_from, addr) in remote_addresses.iter() {
                let remotes_log_result = match addr
                    .send(SyncRemotesMessage {
                        from: server_address.to_owned(),
                        flat_remotes_log: initial_flat_remotes_log.clone(),
                    })
                    .await
                {
                    Ok(send_result) => send_result,
                    Err(err) => {
                        log::error!(
                            "[Remotes.new] Error while passing message to \
GraphClient. Error: '{:?}'",
                            err
                        );
                        Err(StoreError::from(err))
                    }
                };

                let remotes_entry = RemotesEntry {
                    client_addr: (*addr).clone(),
                    remotes_log: remotes_log_result,
                };

                remotes.insert((*remote_from).to_owned(), remotes_entry);
            }

            log::info!(
                "Remotes initialized with initial remotes: '{:?}'",
                remotes
            );

            remotes
        });

        let actor_future =
            future.into_actor(self).map(|remotes, actor, _ctx| {
                actor.remotes = remotes;
                Ok(())
            });

        Box::pin(actor_future)
    }
}

impl<N, E, I> Handler<MutationsLogQuery<N, E, I>> for Remotes<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Result = ResponseActFuture<
        Self,
        Result<Vec<MutationsLogMutation<N, E, I>>, StoreError>,
    >;

    fn handle(
        &mut self,
        _msg: MutationsLogQuery<N, E, I>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let clients: Vec<Addr<GraphClient<N, E, I>>> = self
            .remotes
            .clone()
            .into_iter()
            .map(|(_, entry)| entry.client_addr)
            .collect();

        async move {
            let mut mutations_log_agg = Vec::new();

            for client in clients {
                let mut mutations_log =
                    client.send(MutationsLogQuery::full()).await??;

                // Hashes are generated from contents of GraphMutations
                // the probability of two hashes actually referring to
                // different mutations is acceptably low.
                mutations_log_agg.append(&mut mutations_log);
            }

            mutations_log_agg.sort_by(|a, b| a.hash.cmp(&b.hash));
            mutations_log_agg.dedup_by(|a, b| a.hash == b.hash);
            Ok(mutations_log_agg)
        }
        .interop_actor_boxed(self)
    }
}
