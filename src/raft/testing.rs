use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use arc_swap::ArcSwap;
use fjall::{PersistMode, SingleWriterTxDatabase as TxDatabase};
use tokio::sync::watch;
use uuid::Uuid;

use super::net::{
    LocalPeer, PeerAuth, PeerGuard, PeerListener, PeerNetworkFactory, PeerReceiver, PeerRegistry,
    PeerServer, TonicConnector, spawn_peer_listener,
};
use super::{ClusterNode, Raft, RaftLogStore, StateMachine, TypeConfig};
use crate::cluster::{stamp_cluster_id, verify_or_stamp_identity};
use crate::config::{ClusterConfig, Config};
use crate::metrics::Metrics;
use crate::storage::{
    AdminFold, AdminSnapshot, ApplyCore, Keyspaces, QueueNotifiers, StampClamp, StorageParams,
    Store, rebuild_indexes,
};

pub struct TestNodeOpts {
    pub dir: PathBuf,
    pub node_id: u64,
    pub cluster_id: Uuid,
    pub listen_addr: SocketAddr,
    pub peer_auth_keys: Option<Vec<String>>,
    pub heartbeat_ms: u64,
    pub election_min_ms: u64,
    pub election_max_ms: u64,
}

impl TestNodeOpts {
    pub fn new(dir: PathBuf, node_id: u64, cluster_id: Uuid) -> Self {
        Self {
            dir,
            node_id,
            cluster_id,
            listen_addr: "127.0.0.1:0".parse().expect("loopback addr"),
            peer_auth_keys: None,
            heartbeat_ms: 50,
            election_min_ms: 300,
            election_max_ms: 600,
        }
    }
}

pub struct TestNode {
    pub raft: Raft,
    pub addr: SocketAddr,
    pub node_id: u64,
    pub instance_uuid: Uuid,
    pub registry: PeerRegistry,
    pub local: LocalPeer,
    listener: PeerListener,
    shutdown: watch::Sender<bool>,
}

impl TestNode {
    pub async fn start(opts: TestNodeOpts) -> Result<TestNode, Box<dyn Error>> {
        let db = TxDatabase::builder(&opts.dir).open()?;
        let db_path = opts.dir.to_str().ok_or("utf-8 test dir")?.to_owned();

        let cluster_config = ClusterConfig {
            enabled: true,
            node_id: opts.node_id,
            peer_listen_addr: opts.listen_addr,
            peer_auth_keys: opts.peer_auth_keys.clone(),
            ..Default::default()
        };
        let identity = verify_or_stamp_identity(&db, &cluster_config, &db_path)?;
        stamp_cluster_id(&db, opts.cluster_id)?;

        let log_store = RaftLogStore::open(db.clone(), PersistMode::Buffer)?;

        // Stands in for PR 8's IO loop: flushes whenever an append parks a
        // callback so the engine's commit path advances.
        let pump = log_store.clone();
        tokio::spawn(async move {
            loop {
                pump.flush_wanted().await;
                let store = pump.clone();
                if tokio::task::spawn_blocking(move || store.flush())
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        let store = Store::new(
            db.clone(),
            Keyspaces::open(&db)?,
            StorageParams {
                persist_mode: PersistMode::Buffer,
                sweep_limit: 100,
                dead_letter_retention_ms: 0,
                admin_enabled: false,
            },
            Metrics::new(false),
        );
        let indexes = rebuild_indexes(&store)?;
        let core = ApplyCore::new(
            store,
            indexes,
            QueueNotifiers::default(),
            StampClamp::new(0),
        )
        .map_err(|e| e.to_string())?;

        let (sm_tx, sm_rx) = flume::bounded(4);
        let (_reads_tx, reads_rx) = flume::bounded(4);
        let admin = AdminFold::new(
            false,
            Arc::new(ArcSwap::from_pointee(AdminSnapshot::default())),
        );
        std::thread::Builder::new()
            .name(format!("sepp-test-committer-{}", opts.node_id))
            .spawn(move || core.run_raft(sm_rx, reads_rx, admin))?;

        let sm = StateMachine::new(db, sm_tx, &db_path, 16 << 20)?;

        let engine_config = openraft::Config {
            cluster_name: "sepp-test".into(),
            heartbeat_interval: opts.heartbeat_ms,
            election_timeout_min: opts.election_min_ms,
            election_timeout_max: opts.election_max_ms,
            snapshot_policy: openraft::SnapshotPolicy::Never,
            install_snapshot_timeout: 60_000,
            ..Default::default()
        }
        .validate()?;

        let local = LocalPeer::new(
            opts.node_id,
            identity.instance_uuid,
            Some(opts.cluster_id),
            Config::default().uniform_config_hash(),
        );
        let connector = TonicConnector::new(local.clone(), &cluster_config)?;

        let raft = openraft::Raft::<TypeConfig>::new(
            opts.node_id,
            Arc::new(engine_config),
            PeerNetworkFactory(connector),
            log_store,
            sm,
        )
        .await?;

        let registry = PeerRegistry::default();
        let receiver = Arc::new(PeerReceiver::new(
            raft.clone(),
            PeerGuard::new(local.clone(), registry.clone()),
        ));
        let server = PeerServer::new(receiver, PeerAuth::new(&opts.peer_auth_keys));

        let (shutdown, shutdown_rx) = watch::channel(false);
        let listener = spawn_peer_listener(&cluster_config, 16 << 20, server, shutdown_rx).await?;

        Ok(TestNode {
            raft,
            addr: listener.addr,
            node_id: opts.node_id,
            instance_uuid: identity.instance_uuid,
            registry,
            local,
            listener,
            shutdown,
        })
    }

    // The membership record other nodes dial this node by.
    pub fn cluster_node(&self) -> ClusterNode {
        ClusterNode {
            peer_addr: format!("127.0.0.1:{}", self.addr.port()),
            client_addr: format!("127.0.0.1:{}", self.addr.port()),
        }
    }

    pub async fn shutdown(self) {
        let _ = self.raft.shutdown().await;
        let _ = self.shutdown.send(true);
        let _ = self.listener.handle.await;
    }
}
