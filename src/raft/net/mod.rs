use std::future::Future;
use std::path::PathBuf;
use std::time::Duration;

use openraft::error::{
    Fatal, NetworkError, RPCError, RaftError, ReplicationClosed, StreamingError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::SnapshotResponse;
use prost::Message;

use super::{
    AppendEntriesResponse, ClusterNode, NodeId, TypeConfig, append_response_from_proto,
    entry_to_proto, log_id_to_proto, snapshot_meta_to_proto, vote_request_to_proto,
    vote_response_from_proto, vote_to_proto,
};
use crate::op::OP_FORMAT_VERSION;
use crate::pb::sepp::raft::v1 as pb;

pub mod dispatch;
pub mod grpc;
#[cfg(test)]
mod wire_tests;

pub use dispatch::{LocalPeer, PeerGuard, PeerMeta, PeerReceiver, PeerRegistry, Refusal};
pub use grpc::{PeerAuth, PeerListener, PeerServer, TonicConnector, spawn_peer_listener};

// One wire AppendEntries RPC packs entries under this budget. A single entry
// above it still travels alone: the listener's decoding cap derives from
// limits.max_message_bytes (snapshot::frame_cap), so any legal op fits and
// oversized entries can never wedge replication.
pub(crate) const APPEND_CHUNK_BUDGET_BYTES: usize = 4 * 1024 * 1024;

// Snapshot files stream in fixed-size chunks; the file's trailer checksum
// authenticates reassembly.
pub(crate) const SNAPSHOT_CHUNK_BYTES: usize = 1024 * 1024;

// Dial timeout for peer connections.
pub(crate) const PEER_DIAL_TIMEOUT: Duration = Duration::from_secs(3);

// Per-call options a PeerClient implementation applies to one RPC.
#[derive(Debug, Clone, Default)]
pub struct CallOpts {
    pub timeout: Option<Duration>,
}

// A failed peer RPC, classified for retry policy and operator display.
#[derive(Debug, Clone)]
pub enum PeerError {
    // Could not reach the peer; back off before retrying.
    Unreachable(String),
    // The peer refused our peer_auth key.
    AuthFailed(String),
    // A guard-rail refusal: foreign cluster, identity mismatch, peer not
    // initialized.
    Refused(String),
    // The peer answered outside the taxonomy above: a malformed response,
    // an engine error, a bug.
    Unexpected(String),
}

impl std::fmt::Display for PeerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerError::Unreachable(m) => write!(f, "peer unreachable: {m}"),
            PeerError::AuthFailed(m) => write!(f, "peer auth failed: {m}"),
            PeerError::Refused(m) => write!(f, "peer refused: {m}"),
            PeerError::Unexpected(m) => write!(f, "unexpected peer error: {m}"),
        }
    }
}

impl std::error::Error for PeerError {}

// The client half of the peer transport, one instance per target node.
pub trait PeerClient: Send + Sync + 'static {
    fn handshake(
        &self,
        hello: pb::PeerHello,
        opts: CallOpts,
    ) -> impl Future<Output = Result<pb::PeerHello, PeerError>> + Send;

    fn append_entries(
        &self,
        req: pb::AppendEntriesRequest,
        opts: CallOpts,
    ) -> impl Future<Output = Result<pb::AppendEntriesResponse, PeerError>> + Send;

    fn vote(
        &self,
        req: pb::VoteRequest,
        opts: CallOpts,
    ) -> impl Future<Output = Result<pb::VoteResponse, PeerError>> + Send;

    // Streams the snapshot file and resolves once the peer has installed it
    // (or refused). No CallOpts: the engine's cancel future bounds it, and a
    // deadline would have to cover stream plus install (minutes).
    fn install_snapshot(
        &self,
        start: pb::InstallSnapshotStart,
        file: PathBuf,
    ) -> impl Future<Output = Result<pb::Vote, PeerError>> + Send;

    fn trigger_elect(&self, opts: CallOpts) -> impl Future<Output = Result<(), PeerError>> + Send;
}

pub trait PeerConnector: Send + Sync + 'static {
    type Client: PeerClient;

    fn connect(&self, target: NodeId, node: &ClusterNode) -> Self::Client;
}

pub struct PeerNetworkFactory<F>(pub F);

impl<F: PeerConnector> RaftNetworkFactory<TypeConfig> for PeerNetworkFactory<F> {
    type Network = PeerNetwork<F::Client>;

    async fn new_client(&mut self, target: NodeId, node: &ClusterNode) -> Self::Network {
        PeerNetwork {
            client: self.0.connect(target, node),
            chunk_budget: APPEND_CHUNK_BUDGET_BYTES,
        }
    }
}

type NetRpcError = RPCError<NodeId, ClusterNode, RaftError<NodeId>>;

fn rpc_error(e: PeerError) -> NetRpcError {
    match &e {
        PeerError::Unreachable(_) | PeerError::AuthFailed(_) | PeerError::Refused(_) => {
            RPCError::Unreachable(Unreachable::new(&e))
        }
        PeerError::Unexpected(_) => RPCError::Network(NetworkError::new(&e)),
    }
}

fn decode_error(e: tonic::Status) -> NetRpcError {
    RPCError::Network(NetworkError::new(&PeerError::Unexpected(format!(
        "malformed response: {}",
        e.message()
    ))))
}

pub struct PeerNetwork<C: PeerClient> {
    client: C,
    chunk_budget: usize,
}

impl<C: PeerClient> RaftNetwork<TypeConfig> for PeerNetwork<C> {
    async fn append_entries(
        &mut self,
        rpc: super::AppendEntriesRequest,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse, NetRpcError> {
        let opts = CallOpts {
            timeout: Some(option.hard_ttl()),
        };
        let vote = vote_to_proto(&rpc.vote);
        let leader_commit = rpc.leader_commit.as_ref().map(log_id_to_proto);
        let mut prev = rpc.prev_log_id.as_ref().map(log_id_to_proto);

        let mut remaining: Vec<pb::Entry> = rpc.entries.iter().map(entry_to_proto).collect();

        // Heartbeat or a probe: nothing to chunk.
        if remaining.is_empty() {
            let req = pb::AppendEntriesRequest {
                vote: Some(vote),
                prev_log_id: prev,
                entries: Vec::new(),
                leader_commit,
                op_format_version: OP_FORMAT_VERSION,
            };

            let resp = self
                .client
                .append_entries(req, opts)
                .await
                .map_err(rpc_error)?;
            return append_response_from_proto(resp).map_err(decode_error);
        }

        let mut first_chunk = true;
        while !remaining.is_empty() {
            let mut size = remaining[0].encoded_len();
            let mut end = 1;

            while end < remaining.len() {
                let next = remaining[end].encoded_len();
                if size + next > self.chunk_budget {
                    break;
                }

                size += next;
                end += 1;
            }
            let rest = remaining.split_off(end);
            let chunk = std::mem::replace(&mut remaining, rest);
            let chunk_last = chunk.last().and_then(|e| e.log_id);

            let req = pb::AppendEntriesRequest {
                vote: Some(vote),
                prev_log_id: prev,
                entries: chunk,
                leader_commit,
                op_format_version: OP_FORMAT_VERSION,
            };
            let resp = self
                .client
                .append_entries(req, opts.clone())
                .await
                .map_err(rpc_error)?;

            match append_response_from_proto(resp).map_err(decode_error)? {
                AppendEntriesResponse::Success => {
                    prev = chunk_last;
                    first_chunk = false;
                }

                // The follower accepted a prefix. Its matched id is >= this
                // chunk's prev >= the original prev, so the contract holds.
                partial @ AppendEntriesResponse::PartialSuccess(_) => return Ok(partial),
                AppendEntriesResponse::Conflict if first_chunk => {
                    return Ok(AppendEntriesResponse::Conflict);
                }

                // A mid-stream conflict means the follower truncated between
                // chunks (a competing leader interleaved). The composite
                // request has no honest single answer, so error and let the
                // engine retry from scratch.
                AppendEntriesResponse::Conflict => {
                    return Err(RPCError::Network(NetworkError::new(
                        &PeerError::Unexpected("follower log changed between append chunks".into()),
                    )));
                }

                higher @ AppendEntriesResponse::HigherVote(_) => return Ok(higher),
            }
        }

        Ok(AppendEntriesResponse::Success)
    }

    async fn vote(
        &mut self,
        rpc: super::VoteRequest,
        option: RPCOption,
    ) -> Result<super::VoteResponse, NetRpcError> {
        let opts = CallOpts {
            timeout: Some(option.hard_ttl()),
        };
        let resp = self
            .client
            .vote(vote_request_to_proto(&rpc), opts)
            .await
            .map_err(rpc_error)?;

        vote_response_from_proto(resp).map_err(decode_error)
    }

    async fn full_snapshot(
        &mut self,
        vote: super::Vote,
        snapshot: openraft::Snapshot<TypeConfig>,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<NodeId>, StreamingError<TypeConfig, Fatal<NodeId>>> {
        let start = pb::InstallSnapshotStart {
            vote: Some(vote_to_proto(&vote)),
            meta: Some(snapshot_meta_to_proto(&snapshot.meta)),
        };
        let send = self
            .client
            .install_snapshot(start, snapshot.snapshot.path.clone());

        // Dropping the send future aborts the stream mid-flight, which is
        // exactly what cancellation means to the receiver: a torn file that
        // fails verification.
        tokio::select! {
            closed = cancel => Err(StreamingError::Closed(closed)),
            resp = send => match resp {
                Ok(v) => Ok(SnapshotResponse::new(super::vote_from_proto(&v))),
                Err(e @ (PeerError::Unreachable(_) | PeerError::AuthFailed(_) | PeerError::Refused(_))) => {
                    Err(StreamingError::Unreachable(Unreachable::new(&e)))
                }
                Err(e) => Err(StreamingError::Network(NetworkError::new(&e))),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use openraft::LeaderId;

    use super::super::{Entry, LogId, Vote};
    use super::*;
    use crate::op::Op;
    use openraft::EntryPayload;

    // Records every append request and replays scripted responses (Success
    // once the script runs dry).
    #[derive(Clone, Default)]
    struct FakeClient {
        sent: Arc<Mutex<Vec<pb::AppendEntriesRequest>>>,
        script: Arc<Mutex<VecDeque<pb::AppendEntriesResponse>>>,
    }

    fn success() -> pb::AppendEntriesResponse {
        pb::AppendEntriesResponse {
            result: Some(pb::append_entries_response::Result::Success(pb::Blank {})),
        }
    }

    impl PeerClient for FakeClient {
        async fn handshake(
            &self,
            _hello: pb::PeerHello,
            _opts: CallOpts,
        ) -> Result<pb::PeerHello, PeerError> {
            unimplemented!()
        }

        async fn append_entries(
            &self,
            req: pb::AppendEntriesRequest,
            _opts: CallOpts,
        ) -> Result<pb::AppendEntriesResponse, PeerError> {
            self.sent.lock().unwrap().push(req);
            Ok(self
                .script
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(success))
        }

        async fn vote(
            &self,
            _req: pb::VoteRequest,
            _opts: CallOpts,
        ) -> Result<pb::VoteResponse, PeerError> {
            unimplemented!()
        }

        async fn install_snapshot(
            &self,
            _start: pb::InstallSnapshotStart,
            _file: PathBuf,
        ) -> Result<pb::Vote, PeerError> {
            unimplemented!()
        }

        async fn trigger_elect(&self, _opts: CallOpts) -> Result<(), PeerError> {
            unimplemented!()
        }
    }

    fn log_id(index: u64) -> LogId {
        LogId {
            leader_id: LeaderId::new(1, 1),
            index,
        }
    }

    fn op_entry(index: u64, job_id: &str) -> Entry {
        Entry {
            log_id: log_id(index),
            payload: EntryPayload::Normal(Op::Ack {
                job_id: job_id.into(),
                attempt: 1,
            }),
        }
    }

    fn request(entries: Vec<Entry>) -> super::super::AppendEntriesRequest {
        super::super::AppendEntriesRequest {
            vote: Vote::new_committed(1, 1),
            prev_log_id: Some(log_id(0)),
            entries,
            leader_commit: Some(log_id(0)),
        }
    }

    fn network(client: &FakeClient, chunk_budget: usize) -> PeerNetwork<FakeClient> {
        PeerNetwork {
            client: client.clone(),
            chunk_budget,
        }
    }

    fn opt() -> RPCOption {
        RPCOption::new(Duration::from_secs(1))
    }

    #[tokio::test]
    async fn append_chunks_under_the_budget_and_threads_prev() {
        let client = FakeClient::default();
        let entries: Vec<Entry> = (1..=5).map(|i| op_entry(i, "a-job-id")).collect();
        let per_entry = entry_to_proto(&entries[0]).encoded_len();

        // Budget fits exactly two entries per chunk: 5 entries = 3 RPCs.
        let mut net = network(&client, per_entry * 2);
        let resp = net
            .append_entries(request(entries), opt())
            .await
            .expect("append");
        assert_eq!(resp, AppendEntriesResponse::Success);

        let sent = client.sent.lock().unwrap();
        let split: Vec<(u64, Vec<u64>)> = sent
            .iter()
            .map(|req| {
                (
                    req.prev_log_id.expect("prev is always set here").index,
                    req.entries
                        .iter()
                        .map(|e| e.log_id.expect("entries carry ids").index)
                        .collect(),
                )
            })
            .collect();
        assert_eq!(
            split,
            vec![(0, vec![1, 2]), (2, vec![3, 4]), (4, vec![5]),],
            "each chunk's prev is the previous chunk's last entry",
        );
        for req in sent.iter() {
            assert_eq!(req.vote, Some(vote_to_proto(&Vote::new_committed(1, 1))));
            assert_eq!(req.leader_commit.map(|l| l.index), Some(0));
            assert_eq!(req.op_format_version, OP_FORMAT_VERSION);
        }
    }

    #[tokio::test]
    async fn an_entry_over_the_budget_still_travels_alone() {
        let client = FakeClient::default();
        let entries = vec![
            op_entry(1, "small"),
            op_entry(2, &"x".repeat(1000)),
            op_entry(3, "small"),
        ];

        let mut net = network(&client, 64);
        let resp = net
            .append_entries(request(entries), opt())
            .await
            .expect("append");
        assert_eq!(resp, AppendEntriesResponse::Success);

        let sent = client.sent.lock().unwrap();
        let sizes: Vec<usize> = sent.iter().map(|r| r.entries.len()).collect();
        assert_eq!(sizes, vec![1, 1, 1], "the oversized entry gets its own RPC");
    }

    #[tokio::test]
    async fn a_heartbeat_is_a_single_empty_request() {
        let client = FakeClient::default();
        let mut net = network(&client, 64);
        let resp = net
            .append_entries(request(Vec::new()), opt())
            .await
            .expect("append");
        assert_eq!(resp, AppendEntriesResponse::Success);
        let sent = client.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert!(sent[0].entries.is_empty());
    }

    #[tokio::test]
    async fn partial_success_short_circuits_later_chunks() {
        let client = FakeClient::default();
        client.script.lock().unwrap().extend([
            success(),
            pb::AppendEntriesResponse {
                result: Some(pb::append_entries_response::Result::PartialSuccess(
                    pb::PartialSuccess {
                        matched: Some(log_id_to_proto(&log_id(2))),
                    },
                )),
            },
        ]);

        let entries: Vec<Entry> = (1..=3).map(|i| op_entry(i, "a-job-id")).collect();
        let mut net = network(&client, 1);
        let resp = net
            .append_entries(request(entries), opt())
            .await
            .expect("append");
        assert_eq!(resp, AppendEntriesResponse::PartialSuccess(Some(log_id(2))));
        assert_eq!(
            client.sent.lock().unwrap().len(),
            2,
            "the third chunk is never sent"
        );
    }

    #[tokio::test]
    async fn conflict_on_the_first_chunk_passes_through() {
        let client = FakeClient::default();
        client
            .script
            .lock()
            .unwrap()
            .push_back(pb::AppendEntriesResponse {
                result: Some(pb::append_entries_response::Result::Conflict(pb::Blank {})),
            });

        let entries: Vec<Entry> = (1..=2).map(|i| op_entry(i, "a-job-id")).collect();
        let mut net = network(&client, 1);
        let resp = net
            .append_entries(request(entries), opt())
            .await
            .expect("append");
        assert_eq!(resp, AppendEntriesResponse::Conflict);
        assert_eq!(client.sent.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_mid_stream_conflict_is_a_network_error_not_a_false_conflict() {
        let client = FakeClient::default();
        client.script.lock().unwrap().extend([
            success(),
            pb::AppendEntriesResponse {
                result: Some(pb::append_entries_response::Result::Conflict(pb::Blank {})),
            },
        ]);

        let entries: Vec<Entry> = (1..=2).map(|i| op_entry(i, "a-job-id")).collect();
        let mut net = network(&client, 1);
        let err = net
            .append_entries(request(entries), opt())
            .await
            .expect_err("a conflict against a mid-batch prev must not be reported as Conflict");
        assert!(matches!(err, RPCError::Network(_)), "{err:?}");
    }

    #[tokio::test]
    async fn higher_vote_short_circuits() {
        let client = FakeClient::default();
        let higher = Vote::new(9, 3);
        client
            .script
            .lock()
            .unwrap()
            .push_back(pb::AppendEntriesResponse {
                result: Some(pb::append_entries_response::Result::HigherVote(
                    vote_to_proto(&higher),
                )),
            });

        let entries: Vec<Entry> = (1..=2).map(|i| op_entry(i, "a-job-id")).collect();
        let mut net = network(&client, 1);
        let resp = net
            .append_entries(request(entries), opt())
            .await
            .expect("append");
        assert_eq!(resp, AppendEntriesResponse::HigherVote(higher));
        assert_eq!(client.sent.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn guard_refusals_map_to_unreachable_for_backoff() {
        #[derive(Clone)]
        struct Refusing;
        impl PeerClient for Refusing {
            async fn handshake(
                &self,
                _: pb::PeerHello,
                _: CallOpts,
            ) -> Result<pb::PeerHello, PeerError> {
                unimplemented!()
            }
            async fn append_entries(
                &self,
                _: pb::AppendEntriesRequest,
                _: CallOpts,
            ) -> Result<pb::AppendEntriesResponse, PeerError> {
                Err(PeerError::Refused("foreign cluster".into()))
            }
            async fn vote(
                &self,
                _: pb::VoteRequest,
                _: CallOpts,
            ) -> Result<pb::VoteResponse, PeerError> {
                unimplemented!()
            }
            async fn install_snapshot(
                &self,
                _: pb::InstallSnapshotStart,
                _: PathBuf,
            ) -> Result<pb::Vote, PeerError> {
                unimplemented!()
            }
            async fn trigger_elect(&self, _: CallOpts) -> Result<(), PeerError> {
                unimplemented!()
            }
        }

        let mut net = PeerNetwork {
            client: Refusing,
            chunk_budget: APPEND_CHUNK_BUDGET_BYTES,
        };
        let err = net
            .append_entries(request(Vec::new()), opt())
            .await
            .expect_err("refused");
        assert!(matches!(err, RPCError::Unreachable(_)), "{err:?}");
    }
}
