// Smoldot
// Copyright (C) 2019-2022  Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: GPL-3.0-or-later WITH Classpath-exception-2.0

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

//! Background synchronization service.
//!
//! The [`ConsensusService`] manages a background task dedicated to synchronizing the chain with
//! the network and authoring blocks.
//! Importantly, its design is oriented towards the particular use case of the full node.

// TODO: doc
// TODO: re-review this once finished

use crate::run::{database_thread, jaeger_service, network_service};

use core::{num::NonZeroU32, ops};
use futures::{lock::Mutex, prelude::*};
use hashbrown::HashSet;
use smoldot::{
    // chain::chain_information,
    database::full_sqlite,
    executor,
    header,
    informant::HashDisplay,
    libp2p,
    network::{self, protocol::BlockData},
    sync::all,
};
use std::{collections::BTreeMap, num::NonZeroU64, sync::Arc, time::SystemTime};
use tracing::Instrument as _;

/// Configuration for a [`ConsensusService`].
pub struct Config<'a> {
    /// Closure that spawns background tasks.
    pub tasks_executor: &'a mut dyn FnMut(future::BoxFuture<'static, ()>),

    /// Database to use to read and write information about the chain.
    pub database: Arc<database_thread::DatabaseThread>,

    /// Number of bytes of the block number in the networking protocol.
    pub block_number_bytes: usize,

    /// Hash of the genesis block.
    ///
    /// > **Note**: At the time of writing of this comment, the value in this field is used only
    /// >           to compare against a known genesis hash and print a warning.
    pub genesis_block_hash: [u8; 32],

    /// Access to the network, and index of the chain to sync from the point of view of the
    /// network service.
    pub network_service: (Arc<network_service::NetworkService>, usize),

    /// Receiver for events coming from the network, as returned by
    /// [`network_service::NetworkService::new`].
    pub network_events_receiver: stream::BoxStream<'static, network_service::Event>,

    /// Service to use to report traces.
    pub jaeger_service: Arc<jaeger_service::JaegerService>,
}

/// Identifier for a blocks request to be performed.
#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct BlocksRequestId(usize);

/// Summary of the state of the [`ConsensusService`].
#[derive(Debug, Clone)]
pub struct SyncState {
    pub best_block_number: u64,
    pub best_block_hash: [u8; 32],
    pub finalized_block_number: u64,
    pub finalized_block_hash: [u8; 32],
}

/// Background task that verifies blocks and emits requests.
pub struct ConsensusService {
    /// State kept up-to-date with the background task.
    sync_state: Arc<Mutex<SyncState>>,
}

impl ConsensusService {
    /// Initializes the [`ConsensusService`] with the given configuration.
    #[tracing::instrument(level = "trace", skip(config))]
    pub async fn new(config: Config<'_>) -> Arc<Self> {
        // Perform the initial access to the database to load a bunch of information.
        let (
            finalized_block_hash,
            finalized_block_number,
            best_block_hash,
            best_block_number,
            finalized_block_storage,
            finalized_chain_information,
        ): (_, _, _, _, BTreeMap<Vec<u8>, Vec<u8>>, _) = config
            .database
            .with_database({
                let block_number_bytes = config.block_number_bytes;
                move |database| {
                    let finalized_block_hash = database.finalized_block_hash().unwrap();
                    let finalized_block_number = header::decode(
                        &database
                            .block_scale_encoded_header(&finalized_block_hash)
                            .unwrap()
                            .unwrap(),
                        block_number_bytes,
                    )
                    .unwrap()
                    .number;
                    let best_block_hash = database.best_block_hash().unwrap();
                    let best_block_number = header::decode(
                        &database
                            .block_scale_encoded_header(&best_block_hash)
                            .unwrap()
                            .unwrap(),
                        block_number_bytes,
                    )
                    .unwrap()
                    .number;
                    let finalized_block_storage = database
                        .finalized_block_storage_top_trie(&finalized_block_hash)
                        .unwrap();
                    let finalized_chain_information = database
                        .to_chain_information(&finalized_block_hash)
                        .unwrap();
                    (
                        finalized_block_hash,
                        finalized_block_number,
                        best_block_hash,
                        best_block_number,
                        finalized_block_storage,
                        finalized_chain_information,
                    )
                }
            })
            .await;

        // The Kusama chain contains a fork hardcoded in the official Polkadot client.
        // See <https://github.com/paritytech/polkadot/blob/93f45f996a3d5592a57eba02f91f2fc2bc5a07cf/node/service/src/grandpa_support.rs#L111-L216>
        // Because we don't want to support this in smoldot, a warning is printed instead if we
        // recognize Kusama.
        // See also <https://github.com/paritytech/smoldot/issues/1866>.
        if config.genesis_block_hash
            == [
                176, 168, 212, 147, 40, 92, 45, 247, 50, 144, 223, 183, 230, 31, 135, 15, 23, 180,
                24, 1, 25, 122, 20, 156, 169, 54, 84, 73, 158, 163, 218, 254,
            ]
            && finalized_block_number <= 1500988
        {
            tracing::warn!(
                "The Kusama chain is known to be borked at block #1491596. The official Polkadot \
                client works around this issue by hardcoding a fork in its source code. Smoldot \
                does not support this hardcoded fork and will thus fail to sync past this block."
            );
        }

        let sync_state = Arc::new(Mutex::new(SyncState {
            best_block_number,
            best_block_hash,
            finalized_block_number,
            finalized_block_hash,
        }));

        // Spawn the background task that synchronizes blocks and updates the database.
        (config.tasks_executor)({
            let sync = all::AllSync::new(all::Config {
                chain_information: finalized_chain_information,
                block_number_bytes: config.block_number_bytes,
                allow_unknown_consensus_engines: false,
                sources_capacity: 32,
                blocks_capacity: {
                    // This is the maximum number of blocks between two consecutive justifications.
                    1024
                },
                max_disjoint_headers: 1024,
                max_requests_per_block: NonZeroU32::new(3).unwrap(),
                download_ahead_blocks: {
                    // Assuming a verification speed of 1k blocks/sec and a 99th download time
                    // percentile of two second, the number of blocks to download ahead of time
                    // in order to not block is 2000.
                    // In practice, however, the verification speed and download speed depend on
                    // the chain and the machine of the user.
                    NonZeroU32::new(2000).unwrap()
                },
                full: Some(all::ConfigFull {
                    finalized_runtime: {
                        // Builds the runtime of the finalized block.
                        // Assumed to always be valid, otherwise the block wouldn't have been saved in the
                        // database, hence the large number of unwraps here.
                        let module = finalized_block_storage.get(&b":code"[..]).unwrap();
                        let heap_pages = executor::storage_heap_pages_to_value(
                            finalized_block_storage
                                .get(&b":heappages"[..])
                                .map(|v| &v[..]),
                        )
                        .unwrap();
                        executor::host::HostVmPrototype::new(executor::host::Config {
                            module,
                            heap_pages,
                            exec_hint: executor::vm::ExecHint::CompileAheadOfTime, // TODO: probably should be decided by the optimisticsync
                            allow_unresolved_imports: false,
                        })
                        .unwrap()
                    },
                }),
            });

            let background_sync = SyncBackground {
                sync,
                finalized_block_storage,
                sync_state: sync_state.clone(),
                network_service: config.network_service.0,
                network_chain_index: config.network_service.1,
                from_network_service: config.network_events_receiver,
                database: config.database,
                peers_source_id_map: Default::default(),
                block_requests_finished: stream::FuturesUnordered::new(),
                jaeger_service: config.jaeger_service,
            };

            Box::pin(background_sync.run().instrument(
                tracing::trace_span!(parent: None, "sync-background", root = %HashDisplay(&finalized_block_hash)),
            ))
        });

        Arc::new(ConsensusService { sync_state })
    }

    /// Returns a summary of the state of the service.
    ///
    /// > **Important**: This doesn't represent the content of the database.
    // TODO: maybe remove this in favour of the database; seems like a better idea
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn sync_state(&self) -> SyncState {
        self.sync_state.lock().await.clone()
    }
}

struct SyncBackground {
    /// State machine containing the list of all the peers, all the non-finalized blocks, and all
    /// the network requests in progress.
    ///
    /// Each peer holds an `Option<PeerId>` containing either its `PeerId` for a networking peer,
    /// or `None` if this is the "special peer" representing the local block authoring. Only one
    /// peer must contain `None` and its id must be [`SyncBackground::block_author_sync_source`].
    ///
    /// Each on-going request has a corresponding future within
    /// [`SyncBackground::block_requests_finished`]. This future is wrapped within an aborter, and
    /// the an `AbortHandle` is held within this state machine. It can be used to abort the
    /// request if necessary.
    sync: all::AllSync<future::AbortHandle, Option<libp2p::PeerId>, ()>,

    /// Holds, in parallel of the database, the storage of the latest finalized block.
    /// At the time of writing, this state is stable around `~3MiB` for Polkadot, meaning that it is
    /// completely acceptable to hold it entirely in memory.
    // While reading the storage from the database is an option, doing so considerably slows down
    /// the verification, and also makes it impossible to insert blocks in the database in
    /// parallel of this verification.
    finalized_block_storage: BTreeMap<Vec<u8>, Vec<u8>>,

    sync_state: Arc<Mutex<SyncState>>,

    /// Service managing the connections to the networking peers.
    network_service: Arc<network_service::NetworkService>,

    /// Index, within the [`SyncBackground::network_service`], of the chain that this sync service
    /// is syncing from. This value must be passed as parameter when starting requests on the
    /// network service.
    network_chain_index: usize,

    /// Stream of events coming from the [`SyncBackground::network_service`]. Used to know what
    /// happens on the peer-to-peer network.
    from_network_service: stream::BoxStream<'static, network_service::Event>,

    /// For each networking peer, the identifier of the source in [`SyncBackground::sync`].
    /// This map is kept up-to-date with the "chain connections" of the network service. Whenever
    /// a connection is established with a peer, an entry is inserted in this map and a source is
    /// added to [`SyncBackground::sync`], and whenever a connection is closed, the map entry and
    /// source are removed.
    peers_source_id_map: hashbrown::HashMap<libp2p::PeerId, all::SourceId, fnv::FnvBuildHasher>,

    /// Block requests that have been emitted on the networking service and that are still in
    /// progress. Each entry in this field also has an entry in [`SyncBackground::sync`].
    block_requests_finished: stream::FuturesUnordered<
        future::BoxFuture<
            'static,
            (
                all::RequestId,
                Result<
                    Result<Vec<BlockData>, network_service::BlocksRequestError>,
                    future::Aborted,
                >,
            ),
        >,
    >,

    /// See [`Config::database`].
    database: Arc<database_thread::DatabaseThread>,

    /// How to report events about blocks.
    jaeger_service: Arc<jaeger_service::JaegerService>,
}

impl SyncBackground {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn run(mut self) {
        loop {
            self.start_network_requests().await;
            self = self.process_blocks().await;

            // Update the current best block, used for CLI-related purposes.
            {
                let mut lock = self.sync_state.lock().await;
                lock.best_block_hash = self.sync.best_block_hash();
                lock.best_block_number = self.sync.best_block_number();
            }

            futures::select! {
                network_event = self.from_network_service.next().fuse() => {
                    // We expect the network events channel to never shut down.
                    let network_event = network_event.unwrap();

                    match network_event {
                        network_service::Event::Connected { peer_id, chain_index, best_block_number, best_block_hash }
                            if chain_index == self.network_chain_index =>
                        {
                            let id = self.sync.add_source(Some(peer_id.clone()), best_block_number, best_block_hash);
                            self.peers_source_id_map.insert(peer_id, id);
                        },
                        network_service::Event::Disconnected { peer_id, chain_index }
                            if chain_index == self.network_chain_index =>
                        {
                            let id = self.peers_source_id_map.remove(&peer_id).unwrap();
                            let (_, requests) = self.sync.remove_source(id);
                            for (_, abort) in requests {
                                abort.abort();
                            }
                        },
                        network_service::Event::BlockAnnounce { chain_index, peer_id, header, is_best }
                            if chain_index == self.network_chain_index =>
                        {
                            let _jaeger_span = self
                                .jaeger_service
                                .block_announce_process_span(&header.hash(self.sync.block_number_bytes()));

                            let id = *self.peers_source_id_map.get(&peer_id).unwrap();
                            // TODO: log the outcome
                            match self.sync.block_announce(id, header.scale_encoding_vec(self.sync.block_number_bytes()), is_best) {
                                all::BlockAnnounceOutcome::HeaderVerify => {},
                                all::BlockAnnounceOutcome::TooOld { .. } => {},
                                all::BlockAnnounceOutcome::AlreadyInChain => {},
                                all::BlockAnnounceOutcome::NotFinalizedChain => {},
                                all::BlockAnnounceOutcome::Discarded => {},
                                all::BlockAnnounceOutcome::StoredForLater {} => {},
                                all::BlockAnnounceOutcome::InvalidHeader(_) => unreachable!(),
                            }
                        },
                        _ => {
                            // Different chain index.
                        }
                    }
                },

                (request_id, result) = self.block_requests_finished.select_next_some() => {
                    // `result` is an error if the block request got cancelled by the sync state
                    // machine.
                    // TODO: clarify this piece of code
                    if let Ok(result) = result {
                        let result = result.map_err(|_| ());
                        let (_, response_outcome) = self.sync.blocks_request_response(request_id, result.map(|v| v.into_iter().map(|block| all::BlockRequestSuccessBlock {
                            scale_encoded_header: block.header.unwrap(), // TODO: don't unwrap
                            scale_encoded_extrinsics: block.body.unwrap(), // TODO: don't unwrap
                            scale_encoded_justifications: block.justifications.unwrap_or_default(),
                            user_data: (),
                        })));

                        match response_outcome {
                            all::ResponseOutcome::Outdated
                            | all::ResponseOutcome::Queued
                            | all::ResponseOutcome::NotFinalizedChain { .. }
                            | all::ResponseOutcome::AllAlreadyInChain { .. } => {
                            }
                        }
                    }
                },
            }
        }
    }

    /// Starts all the new network requests that should be started.
    // TODO: handle obsolete requests
    async fn start_network_requests(&mut self) {
        loop {
            // `desired_requests()` returns, in decreasing order of priority, the requests
            // that should be started in order for the syncing to proceed. We simply pick the
            // first request, but enforce one ongoing request per source.
            let (source_id, _, mut request_info) =
                match self.sync.desired_requests().find(|(source_id, _, _)| {
                    self.sync.source_num_ongoing_requests(*source_id) == 0
                }) {
                    Some(v) => v,
                    None => break,
                };

            // Before notifying the syncing of the request, clamp the number of blocks to the
            // number of blocks we expect to receive.
            request_info.num_blocks_clamp(NonZeroU64::new(64).unwrap());

            match request_info {
                all::DesiredRequest::BlocksRequest {
                    first_block_hash,
                    first_block_height,
                    ascending,
                    num_blocks,
                    request_headers,
                    request_bodies,
                    request_justification,
                } => {
                    let peer_id = self.sync[source_id].clone().unwrap();

                    // TODO: add jaeger span

                    let request = self.network_service.clone().blocks_request(
                        peer_id,
                        self.network_chain_index,
                        network::protocol::BlocksRequestConfig {
                            start: if let Some(first_block_hash) = first_block_hash {
                                network::protocol::BlocksRequestConfigStart::Hash(first_block_hash)
                            } else {
                                network::protocol::BlocksRequestConfigStart::Number(
                                    first_block_height,
                                )
                            },
                            desired_count: NonZeroU32::new(
                                u32::try_from(num_blocks.get()).unwrap_or(u32::max_value()),
                            )
                            .unwrap(),
                            direction: if ascending {
                                network::protocol::BlocksRequestDirection::Ascending
                            } else {
                                network::protocol::BlocksRequestDirection::Descending
                            },
                            fields: network::protocol::BlocksRequestFields {
                                header: request_headers,
                                body: request_bodies,
                                justifications: request_justification,
                            },
                        },
                    );

                    let (request, abort) = future::abortable(request);
                    let request_id = self.sync.add_request(source_id, request_info.into(), abort);

                    self.block_requests_finished
                        .push(request.map(move |r| (request_id, r)).boxed());
                }
                all::DesiredRequest::GrandpaWarpSync { .. }
                | all::DesiredRequest::StorageGet { .. }
                | all::DesiredRequest::RuntimeCallMerkleProof { .. } => {
                    // Not used in "full" mode.
                    unreachable!()
                }
            }
        }
    }

    async fn process_blocks(mut self) -> Self {
        // The sync state machine can be in a few various states. At the time of writing:
        // idle, verifying header, verifying block, verifying grandpa warp sync proof,
        // verifying storage proof.
        // If the state is one of the "verifying" states, perform the actual verification and
        // loop again until the sync is in an idle state.
        loop {
            let unix_time = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap();

            match self.sync.process_one() {
                all::ProcessOne::AllSync(idle) => {
                    self.sync = idle;
                    break;
                }
                all::ProcessOne::VerifyWarpSyncFragment(_)
                | all::ProcessOne::WarpSyncError { .. }
                | all::ProcessOne::WarpSyncFinished { .. } => unreachable!(),
                all::ProcessOne::VerifyBodyHeader(verify) => {
                    let hash_to_verify = verify.hash();
                    let height_to_verify = verify.height();
                    let scale_encoded_header_to_verify = verify.scale_encoded_header().to_owned(); // TODO: copy :-/

                    let span = tracing::debug_span!(
                        "block-verification",
                        hash_to_verify = %HashDisplay(&hash_to_verify), height = %height_to_verify,
                        outcome = tracing::field::Empty, is_new_best = tracing::field::Empty,
                        error = tracing::field::Empty,
                    );
                    let _enter = span.enter();
                    let _jaeger_span = self.jaeger_service.block_body_verify_span(&hash_to_verify);

                    let mut verify = verify.start(unix_time, ());
                    // TODO: check this block against the chain spec's badBlocks
                    loop {
                        match verify {
                            all::BlockVerification::Error {
                                sync: sync_out,
                                error,
                                ..
                            } => {
                                // Print a separate warning because it is important for the user
                                // to be aware of the verification failure.
                                // `%error` is last because it's quite big.
                                tracing::warn!(
                                    parent: &span, hash = %HashDisplay(&hash_to_verify),
                                    height = %height_to_verify, %error,
                                    "failed-block-verification"
                                );
                                span.record("outcome", &"failure");
                                span.record("error", &tracing::field::display(error));
                                self.sync = sync_out;
                                break;
                            }
                            all::BlockVerification::Success {
                                is_new_best,
                                sync: sync_out,
                                ..
                            } => {
                                span.record("outcome", &"success");
                                span.record("is_new_best", &is_new_best);

                                // Processing has made a step forward.

                                if is_new_best {
                                    // Update the networking.
                                    let fut = self.network_service.set_local_best_block(
                                        self.network_chain_index,
                                        sync_out.best_block_hash(),
                                        sync_out.best_block_number(),
                                    );
                                    fut.await;

                                    // Update the externally visible best block state.
                                    let mut lock = self.sync_state.lock().await;
                                    lock.best_block_hash = sync_out.best_block_hash();
                                    lock.best_block_number = sync_out.best_block_number();
                                    drop(lock);
                                }

                                self.sync = sync_out;

                                // Announce the newly-verified block to all the sources that might
                                // not be aware of it. We can never be guaranteed that a certain
                                // source does *not* know about a block, however it is not a big
                                // problem to send a block announce to a source that already knows
                                // about that block. For this reason, the list of sources we send
                                // the block announce to is `all_sources - sources_that_know_it`.
                                //
                                // Note that not sending block announces to sources that already
                                // know that block means that these sources might also miss the
                                // fact that our local best block has been updated. This is in
                                // practice not a problem either.
                                let sources_to_announce_to = {
                                    let mut all_sources =
                                        self.sync
                                            .sources()
                                            .collect::<HashSet<_, fnv::FnvBuildHasher>>();
                                    for knows in self.sync.knows_non_finalized_block(
                                        height_to_verify,
                                        &hash_to_verify,
                                    ) {
                                        all_sources.remove(&knows);
                                    }
                                    all_sources
                                };

                                for source_id in sources_to_announce_to {
                                    let peer_id = match &self.sync[source_id] {
                                        Some(pid) => pid,
                                        None => continue,
                                    };

                                    if self
                                        .network_service
                                        .clone()
                                        .send_block_announce(
                                            peer_id,
                                            0,
                                            &scale_encoded_header_to_verify,
                                            is_new_best,
                                        )
                                        .await
                                        .is_ok()
                                    {
                                        // Note that `try_add_known_block_to_source` might have
                                        // no effect, which is not a problem considering that this
                                        // block tracking is mostly about optimizations and
                                        // politeness.
                                        self.sync.try_add_known_block_to_source(
                                            source_id,
                                            height_to_verify,
                                            hash_to_verify,
                                        );
                                    }
                                }

                                break;
                            }

                            all::BlockVerification::FinalizedStorageGet(req) => {
                                let value = self
                                    .finalized_block_storage
                                    .get(&req.key_as_vec())
                                    .map(|v| &v[..]);
                                verify = req.inject_value(value);
                            }
                            all::BlockVerification::FinalizedStorageNextKey(req) => {
                                // TODO: to_vec() :-/ range() immediately calculates the range of keys so there's no borrowing issue, but the take_while needs to keep req borrowed, which isn't possible
                                let req_key = req.key().as_ref().to_vec();
                                let next_key = self
                                    .finalized_block_storage
                                    .range::<[u8], _>((
                                        ops::Bound::Included(req.key().as_ref()),
                                        ops::Bound::Unbounded,
                                    ))
                                    .find(move |(k, _)| k[..] > req_key[..])
                                    .map(|(k, _)| k);
                                verify = req.inject_key(next_key);
                            }
                            all::BlockVerification::FinalizedStoragePrefixKeys(req) => {
                                // TODO: to_vec() :-/ range() immediately calculates the range of keys so there's no borrowing issue, but the take_while needs to keep req borrowed, which isn't possible
                                let prefix = req.prefix().as_ref().to_vec();
                                let keys = self
                                    .finalized_block_storage
                                    .range::<[u8], _>((
                                        ops::Bound::Included(req.prefix().as_ref()),
                                        ops::Bound::Unbounded,
                                    ))
                                    .take_while(|(k, _)| k.starts_with(&prefix))
                                    .map(|(k, _)| k);
                                verify = req.inject_keys_ordered(keys);
                            }
                            all::BlockVerification::RuntimeCompilation(rt) => {
                                verify = rt.build();
                            }
                        }
                    }
                }

                all::ProcessOne::VerifyFinalityProof(verify) => {
                    let span = tracing::debug_span!(
                        "finality-proof-verification",
                        outcome = tracing::field::Empty,
                        error = tracing::field::Empty,
                    );
                    let _enter = span.enter();

                    match verify.perform(rand::random()) {
                        (
                            sync_out,
                            all::FinalityProofVerifyOutcome::NewFinalized {
                                finalized_blocks,
                                updates_best_block,
                            },
                        ) => {
                            span.record("outcome", &"success");
                            self.sync = sync_out;

                            if updates_best_block {
                                let fut = self.network_service.set_local_best_block(
                                    self.network_chain_index,
                                    self.sync.best_block_hash(),
                                    self.sync.best_block_number(),
                                );
                                fut.await;
                            }

                            let mut lock = self.sync_state.lock().await;
                            lock.best_block_hash = self.sync.best_block_hash();
                            lock.best_block_number = self.sync.best_block_number();
                            drop(lock);

                            if let Some(last_finalized) = finalized_blocks.last() {
                                let mut lock = self.sync_state.lock().await;
                                lock.finalized_block_hash =
                                    last_finalized.header.hash(self.sync.block_number_bytes());
                                lock.finalized_block_number = last_finalized.header.number;
                            }

                            // TODO: maybe write in a separate task? but then we can't access the finalized storage immediately after?
                            for block in &finalized_blocks {
                                for (key, value) in block
                                    .full
                                    .as_ref()
                                    .unwrap()
                                    .storage_top_trie_changes
                                    .diff_iter_unordered()
                                {
                                    if let Some(value) = value {
                                        self.finalized_block_storage
                                            .insert(key.to_owned(), value.to_owned());
                                    } else {
                                        let _was_there = self.finalized_block_storage.remove(key);
                                        // TODO: if a block inserts a new value, then removes it in the next block, the key will remain in `finalized_block_storage`; either solve this or document this
                                        // assert!(_was_there.is_some());
                                    }
                                }
                            }

                            let new_finalized_hash = finalized_blocks
                                .last()
                                .map(|lf| lf.header.hash(self.sync.block_number_bytes()))
                                .unwrap();
                            let block_number_bytes = self.sync.block_number_bytes();
                            database_blocks(&self.database, finalized_blocks, block_number_bytes)
                                .await;
                            database_set_finalized(&self.database, new_finalized_hash).await;
                            continue;
                        }
                        (sync_out, all::FinalityProofVerifyOutcome::GrandpaCommitPending) => {
                            span.record("outcome", &"pending");
                            self.sync = sync_out;
                            continue;
                        }
                        (sync_out, all::FinalityProofVerifyOutcome::AlreadyFinalized) => {
                            span.record("outcome", &"already-finalized");
                            self.sync = sync_out;
                            continue;
                        }
                        (sync_out, all::FinalityProofVerifyOutcome::GrandpaCommitError(error)) => {
                            span.record("outcome", &"failure");
                            span.record("error", &tracing::field::display(error));
                            self.sync = sync_out;
                            continue;
                        }
                        (sync_out, all::FinalityProofVerifyOutcome::JustificationError(error)) => {
                            span.record("outcome", &"failure");
                            span.record("error", &tracing::field::display(error));
                            self.sync = sync_out;
                            continue;
                        }
                    }
                }

                all::ProcessOne::VerifyHeader(verify) => {
                    let hash_to_verify = verify.hash();
                    let height_to_verify = verify.height();

                    let span = tracing::debug_span!(
                        "header-verification",
                        hash_to_verify = %HashDisplay(&hash_to_verify), height = %height_to_verify,
                        outcome = tracing::field::Empty, error = tracing::field::Empty,
                    );
                    let _enter = span.enter();
                    let _jaeger_span = self
                        .jaeger_service
                        .block_header_verify_span(&hash_to_verify);

                    match verify.perform(unix_time, ()) {
                        all::HeaderVerifyOutcome::Success { sync: sync_out, .. } => {
                            span.record("outcome", &"success");
                            self.sync = sync_out;
                            continue;
                        }
                        all::HeaderVerifyOutcome::Error {
                            sync: sync_out,
                            error,
                            ..
                        } => {
                            span.record("outcome", &"failure");
                            span.record("error", &tracing::field::display(error));
                            self.sync = sync_out;
                            continue;
                        }
                    }
                }
            }
        }

        self
    }
}

/// Writes blocks to the database
async fn database_blocks(
    database: &database_thread::DatabaseThread,
    blocks: Vec<all::Block<()>>,
    block_number_bytes: usize,
) {
    database
        .with_database_detached(move |database| {
            for block in blocks {
                // TODO: overhead for building the SCALE encoding of the header
                let result = database.insert(
                    &block.header.scale_encoding(block_number_bytes).fold(
                        Vec::new(),
                        |mut a, b| {
                            a.extend_from_slice(b.as_ref());
                            a
                        },
                    ),
                    true, // TODO: is_new_best?
                    block.full.as_ref().unwrap().body.iter(),
                    block
                        .full
                        .as_ref()
                        .unwrap()
                        .storage_top_trie_changes
                        .diff_iter_unordered(),
                );

                match result {
                    Ok(()) => {}
                    Err(full_sqlite::InsertError::Duplicate) => {} // TODO: this should be an error ; right now we silence them because non-finalized blocks aren't loaded from the database at startup, resulting in them being downloaded again
                    Err(err) => panic!("{}", err),
                }
            }
        })
        .await
}

/// Writes blocks to the database
async fn database_set_finalized(
    database: &database_thread::DatabaseThread,
    finalized_block_hash: [u8; 32],
) {
    // TODO: what if best block changed?
    database
        .with_database_detached(move |database| {
            database.set_finalized(&finalized_block_hash).unwrap();
        })
        .await
}
