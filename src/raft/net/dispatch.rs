use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use tonic::Status;
use uuid::Uuid;

use super::super::{
    Raft, SnapshotFile, append_request_from_proto, append_response_to_proto,
    snapshot_meta_from_proto, vote_from_proto, vote_request_from_proto, vote_response_to_proto,
    vote_to_proto,
};
use crate::op::OP_FORMAT_VERSION;
use crate::pb::sepp::raft::v1 as pb;

pub const METADATA_CLUSTER_ID: &str = "sepp-cluster-id";
pub const METADATA_NODE_ID: &str = "sepp-node-id";
pub const METADATA_INSTANCE_UUID: &str = "sepp-instance-uuid";

pub const METADATA_PEER_ERROR: &str = "sepp-peer-error";
pub const PEER_ERROR_AUTH: &str = "auth";
pub const PEER_ERROR_CLUSTER: &str = "foreign-cluster";
pub const PEER_ERROR_IDENTITY: &str = "identity";
pub const PEER_ERROR_UNINITIALIZED: &str = "uninitialized";

// The identity a sender presented with a peer RPC.
#[derive(Debug, Clone, Default)]
pub struct PeerMeta {
    pub cluster_id: Option<Uuid>,
    pub node_id: Option<u64>,
    pub instance_uuid: Option<Uuid>,
}

// This node's identity as presented to peers. cluster_id lives behind a lock
// because `cluster init` stamps it on a running node.
#[derive(Clone)]
pub struct LocalPeer {
    pub node_id: u64,
    pub instance_uuid: Uuid,
    cluster_id: Arc<RwLock<Option<Uuid>>>,
    pub uniform_config_hash: [u8; 32],
}

impl LocalPeer {
    pub fn new(
        node_id: u64,
        instance_uuid: Uuid,
        cluster_id: Option<Uuid>,
        uniform_config_hash: [u8; 32],
    ) -> Self {
        Self {
            node_id,
            instance_uuid,
            cluster_id: Arc::new(RwLock::new(cluster_id)),
            uniform_config_hash,
        }
    }

    pub fn cluster_id(&self) -> Option<Uuid> {
        *self.cluster_id.read().expect("cluster id lock")
    }

    pub fn set_cluster_id(&self, id: Uuid) {
        *self.cluster_id.write().expect("cluster id lock") = Some(id);
    }

    pub fn hello(&self) -> pb::PeerHello {
        pb::PeerHello {
            cluster_id: self
                .cluster_id()
                .map(|id| id.as_bytes().to_vec())
                .unwrap_or_default(),
            node_id: self.node_id,
            instance_uuid: self.instance_uuid.as_bytes().to_vec(),
            sepp_version: env!("CARGO_PKG_VERSION").into(),
            op_format_version: OP_FORMAT_VERSION,
            uniform_config_hash: self.uniform_config_hash.to_vec(),
        }
    }
}

// (node_id -> instance_uuid) registrations, populated when a node joins
// (`cluster add`) and consulted on every raft RPC. This, not
// openraft, is the production defense against a wiped follower rejoining
// under its old id.
#[derive(Clone, Default)]
pub struct PeerRegistry {
    known: Arc<RwLock<HashMap<u64, Uuid>>>,
}

impl PeerRegistry {
    pub fn register(&self, node_id: u64, instance_uuid: Uuid) {
        self.known
            .write()
            .expect("peer registry lock")
            .insert(node_id, instance_uuid);
    }

    pub fn deregister(&self, node_id: u64) {
        self.known
            .write()
            .expect("peer registry lock")
            .remove(&node_id);
    }

    fn check(&self, node_id: u64, instance_uuid: Uuid) -> Result<(), Refusal> {
        let known = self.known.read().expect("peer registry lock");
        if let Some(registered) = known.get(&node_id)
            && *registered != instance_uuid
        {
            return Err(Refusal::Identity(format!(
                "node {node_id} presented instance_uuid {instance_uuid} but is registered with \
                 {registered}; if its disk was wiped or replaced, remove the node and re-add it",
            )));
        }

        if let Some((other, _)) = known
            .iter()
            .find(|(id, registered)| **id != node_id && **registered == instance_uuid)
        {
            return Err(Refusal::Identity(format!(
                "instance_uuid {instance_uuid} is already registered to node {other}; a data \
                 directory may join a cluster once, not under a second node_id",
            )));
        }

        Ok(())
    }
}

// A guard-rail refusal, mapped to a Status carrying the classifier metadata.
#[derive(Debug)]
pub enum Refusal {
    MissingMeta(&'static str),
    Uninitialized,
    ForeignCluster { theirs: Uuid, ours: Uuid },
    Identity(String),
}

impl From<Refusal> for Status {
    fn from(refusal: Refusal) -> Status {
        let (message, kind) = match refusal {
            Refusal::MissingMeta(key) => (
                format!("peer RPC without {key} metadata"),
                PEER_ERROR_IDENTITY,
            ),
            Refusal::Uninitialized => (
                "cluster enabled but not initialized; run sepp cluster init or add this node \
                 from the leader"
                    .to_string(),
                PEER_ERROR_UNINITIALIZED,
            ),
            Refusal::ForeignCluster { theirs, ours } => (
                format!("peer RPC from cluster {theirs} refused: this node belongs to {ours}"),
                PEER_ERROR_CLUSTER,
            ),
            Refusal::Identity(message) => (message, PEER_ERROR_IDENTITY),
        };

        let mut status = Status::failed_precondition(message);
        status
            .metadata_mut()
            .insert(METADATA_PEER_ERROR, kind.parse().expect("static ascii"));
        status
    }
}

// The guard the raft RPCs (append, vote, snapshot, elect) run before
// passing on the request to the Raft engine. Handshake is exempt: it must answer before init
// and across cluster ids so join pre-checks and operators get a legible
// answer (auth still applies, in the transport).
pub struct PeerGuard {
    local: LocalPeer,
    registry: PeerRegistry,
}

impl PeerGuard {
    pub fn new(local: LocalPeer, registry: PeerRegistry) -> Self {
        Self { local, registry }
    }

    pub fn local(&self) -> &LocalPeer {
        &self.local
    }

    pub fn check_raft(&self, meta: &PeerMeta) -> Result<(), Refusal> {
        let theirs = meta
            .cluster_id
            .ok_or(Refusal::MissingMeta(METADATA_CLUSTER_ID))?;
        let node_id = meta.node_id.ok_or(Refusal::MissingMeta(METADATA_NODE_ID))?;
        let instance_uuid = meta
            .instance_uuid
            .ok_or(Refusal::MissingMeta(METADATA_INSTANCE_UUID))?;

        let ours = self.local.cluster_id().ok_or(Refusal::Uninitialized)?;
        if theirs != ours {
            return Err(Refusal::ForeignCluster { theirs, ours });
        }

        self.registry.check(node_id, instance_uuid)
    }
}

fn engine_fatal(e: impl std::fmt::Display) -> Status {
    Status::internal(format!("raft engine: {e}"))
}

// Wire-request-to-engine dispatch.
pub struct PeerReceiver {
    raft: Raft,
    guard: PeerGuard,
}

impl PeerReceiver {
    pub fn new(raft: Raft, guard: PeerGuard) -> Self {
        Self { raft, guard }
    }

    pub fn guard(&self) -> &PeerGuard {
        &self.guard
    }

    // Exposed so streaming transports can refuse before accepting the body.
    pub fn check_raft(&self, meta: &PeerMeta) -> Result<(), Status> {
        self.guard.check_raft(meta).map_err(Status::from)
    }

    pub async fn handshake(
        &self,
        _meta: &PeerMeta,
        _req: pb::HandshakeRequest,
    ) -> Result<pb::HandshakeResponse, Status> {
        Ok(pb::HandshakeResponse {
            hello: Some(self.guard.local.hello()),
        })
    }

    pub async fn append_entries(
        &self,
        meta: &PeerMeta,
        req: pb::AppendEntriesRequest,
    ) -> Result<pb::AppendEntriesResponse, Status> {
        self.check_raft(meta)?;
        let req = append_request_from_proto(req)?;
        let resp = self.raft.append_entries(req).await.map_err(engine_fatal)?;

        Ok(append_response_to_proto(&resp))
    }

    pub async fn vote(
        &self,
        meta: &PeerMeta,
        req: pb::VoteRequest,
    ) -> Result<pb::VoteResponse, Status> {
        self.check_raft(meta)?;
        let req = vote_request_from_proto(req)?;
        let resp = self.raft.vote(req).await.map_err(engine_fatal)?;

        Ok(vote_response_to_proto(&resp))
    }

    pub async fn trigger_elect(&self, meta: &PeerMeta) -> Result<(), Status> {
        self.check_raft(meta)?;
        self.raft.trigger().elect().await.map_err(engine_fatal)
    }

    // Where an incoming snapshot stream lands; the transport writes the
    // chunks, then hands the finished file to install_snapshot.
    pub async fn begin_snapshot(&self) -> Result<Box<SnapshotFile>, Status> {
        self.raft
            .begin_receiving_snapshot()
            .await
            .map_err(engine_fatal)
    }

    pub async fn install_snapshot(
        &self,
        meta: &PeerMeta,
        start: pb::InstallSnapshotStart,
        file: Box<SnapshotFile>,
    ) -> Result<pb::Vote, Status> {
        self.check_raft(meta)?;
        let vote = vote_from_proto(
            &start
                .vote
                .ok_or_else(|| Status::invalid_argument("snapshot start without vote"))?,
        );

        let snap_meta = snapshot_meta_from_proto(
            start
                .meta
                .ok_or_else(|| Status::invalid_argument("snapshot start without meta"))?,
        );

        let resp = self
            .raft
            .install_full_snapshot(
                vote,
                openraft::Snapshot {
                    meta: snap_meta,
                    snapshot: file,
                },
            )
            .await
            .map_err(engine_fatal)?;

        Ok(vote_to_proto(&resp.vote))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local(cluster_id: Option<Uuid>) -> LocalPeer {
        LocalPeer::new(1, Uuid::from_u128(0x11), cluster_id, [0xab; 32])
    }

    fn meta(cluster_id: Uuid, node_id: u64, instance_uuid: Uuid) -> PeerMeta {
        PeerMeta {
            cluster_id: Some(cluster_id),
            node_id: Some(node_id),
            instance_uuid: Some(instance_uuid),
        }
    }

    fn classifier(refusal: Refusal) -> String {
        let status = Status::from(refusal);
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        status
            .metadata()
            .get(METADATA_PEER_ERROR)
            .expect("refusals carry the classifier")
            .to_str()
            .expect("ascii")
            .to_owned()
    }

    #[test]
    fn raft_rpcs_require_full_identity_metadata() {
        let guard = PeerGuard::new(local(Some(Uuid::from_u128(1))), PeerRegistry::default());
        let err = guard
            .check_raft(&PeerMeta::default())
            .expect_err("missing meta");
        assert!(matches!(err, Refusal::MissingMeta(METADATA_CLUSTER_ID)));
    }

    #[test]
    fn an_uninitialized_node_refuses_raft_rpcs_by_name() {
        let cluster = Uuid::from_u128(1);
        let guard = PeerGuard::new(local(None), PeerRegistry::default());
        let err = guard
            .check_raft(&meta(cluster, 2, Uuid::from_u128(0x22)))
            .expect_err("uninitialized");
        assert!(matches!(err, Refusal::Uninitialized));
        assert_eq!(classifier(err), PEER_ERROR_UNINITIALIZED);
    }

    #[test]
    fn a_foreign_cluster_id_is_refused() {
        let guard = PeerGuard::new(local(Some(Uuid::from_u128(1))), PeerRegistry::default());
        let err = guard
            .check_raft(&meta(Uuid::from_u128(2), 2, Uuid::from_u128(0x22)))
            .expect_err("foreign cluster");
        assert!(matches!(err, Refusal::ForeignCluster { .. }));
        assert_eq!(classifier(err), PEER_ERROR_CLUSTER);
    }

    #[test]
    fn a_matching_peer_passes_with_or_without_registration() {
        let cluster = Uuid::from_u128(1);
        let registry = PeerRegistry::default();
        let guard = PeerGuard::new(local(Some(cluster)), registry.clone());
        let peer = meta(cluster, 2, Uuid::from_u128(0x22));

        guard.check_raft(&peer).expect("unregistered peers pass");
        registry.register(2, Uuid::from_u128(0x22));
        guard.check_raft(&peer).expect("registered peers pass");
    }

    #[test]
    fn a_known_node_id_with_a_new_uuid_is_refused() {
        let cluster = Uuid::from_u128(1);
        let registry = PeerRegistry::default();
        registry.register(2, Uuid::from_u128(0x22));
        let guard = PeerGuard::new(local(Some(cluster)), registry);

        let err = guard
            .check_raft(&meta(cluster, 2, Uuid::from_u128(0x99)))
            .expect_err("a wiped disk restarted under an old id must be refused");
        assert!(matches!(err, Refusal::Identity(_)));
        assert_eq!(classifier(err), PEER_ERROR_IDENTITY);
    }

    #[test]
    fn one_uuid_under_two_node_ids_is_refused() {
        let cluster = Uuid::from_u128(1);
        let registry = PeerRegistry::default();
        registry.register(2, Uuid::from_u128(0x22));
        let guard = PeerGuard::new(local(Some(cluster)), registry);

        let err = guard
            .check_raft(&meta(cluster, 3, Uuid::from_u128(0x22)))
            .expect_err("a copy-pasted data dir must be refused");
        assert!(matches!(err, Refusal::Identity(_)));
    }

    #[test]
    fn deregistration_frees_the_node_id_for_a_fresh_disk() {
        let cluster = Uuid::from_u128(1);
        let registry = PeerRegistry::default();
        registry.register(2, Uuid::from_u128(0x22));
        let guard = PeerGuard::new(local(Some(cluster)), registry.clone());

        registry.deregister(2);
        guard
            .check_raft(&meta(cluster, 2, Uuid::from_u128(0x99)))
            .expect("remove deregisters the uuid so the id is re-addable");
    }

    #[test]
    fn hello_reports_identity_versions_and_config_hash() {
        let peer = local(Some(Uuid::from_u128(1)));
        let hello = peer.hello();
        assert_eq!(hello.cluster_id, Uuid::from_u128(1).as_bytes().to_vec());
        assert_eq!(hello.node_id, 1);
        assert_eq!(
            hello.instance_uuid,
            Uuid::from_u128(0x11).as_bytes().to_vec()
        );
        assert_eq!(hello.sepp_version, env!("CARGO_PKG_VERSION"));
        assert_eq!(hello.op_format_version, OP_FORMAT_VERSION);
        assert_eq!(hello.uniform_config_hash, vec![0xab; 32]);

        let uninitialized = local(None);
        assert!(uninitialized.hello().cluster_id.is_empty());
    }
}
