use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::{AsciiMetadataValue, MetadataMap};
use tonic::transport::server::TcpIncoming;
use tonic::transport::{
    Certificate, Channel, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig,
};
use tonic::{Code, Request, Response, Status, Streaming};
use tracing::{error, info, warn};
use uuid::Uuid;

use super::dispatch::{
    LocalPeer, METADATA_CLUSTER_ID, METADATA_INSTANCE_UUID, METADATA_NODE_ID, METADATA_PEER_ERROR,
    PEER_ERROR_AUTH, PeerMeta, PeerReceiver,
};
use super::{
    CallOpts, PEER_DIAL_TIMEOUT, PeerClient, PeerConnector, PeerError, SNAPSHOT_CHUNK_BYTES,
};
use crate::config::ClusterConfig;
use crate::pb::sepp::raft::v1 as pb;
use crate::pb::sepp::raft::v1::raft_peer_service_client::RaftPeerServiceClient;
use crate::pb::sepp::raft::v1::raft_peer_service_server::{RaftPeerService, RaftPeerServiceServer};
use crate::raft::snapshot::frame_cap;
use crate::raft::{ClusterNode, NodeId};

// Connection settings shared by every outgoing peer client.
#[derive(Clone)]
pub struct TonicConnector {
    local: LocalPeer,
    // First configured key in cluster.peer_auth_keys.
    send_key: Option<AsciiMetadataValue>,
    tls_ca_pem: Option<Vec<u8>>,
}

impl TonicConnector {
    pub fn new(
        local: LocalPeer,
        cluster: &ClusterConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let send_key = match cluster.peer_auth_keys.as_deref() {
            Some([]) | None => None,
            Some([first, ..]) => Some(
                format!("Bearer {first}")
                    .parse()
                    .map_err(|e| format!("cluster.peer_auth_keys[0]: {e}"))?,
            ),
        };

        let tls_ca_pem = cluster
            .peer_tls_ca_path
            .as_deref()
            .map(|path| {
                std::fs::read(path)
                    .map_err(|e| format!("reading cluster.peer_tls_ca_path ({path}): {e}"))
            })
            .transpose()?;

        Ok(Self {
            local,
            send_key,
            tls_ca_pem,
        })
    }
}

impl PeerConnector for TonicConnector {
    type Client = TonicPeerClient;

    fn connect(&self, _target: NodeId, node: &ClusterNode) -> TonicPeerClient {
        TonicPeerClient {
            addr: node.peer_addr.clone(),
            local: self.local.clone(),
            send_key: self.send_key.clone(),
            tls_ca_pem: self.tls_ca_pem.clone(),
            channel: tokio::sync::Mutex::new(None),
        }
    }
}

pub struct TonicPeerClient {
    addr: String,
    local: LocalPeer,
    send_key: Option<AsciiMetadataValue>,
    tls_ca_pem: Option<Vec<u8>>,
    // Lazily dialed and then reused; tonic channels reconnect on demand.
    channel: tokio::sync::Mutex<Option<RaftPeerServiceClient<Channel>>>,
}

impl TonicPeerClient {
    async fn conn(&self) -> Result<RaftPeerServiceClient<Channel>, PeerError> {
        let mut cached = self.channel.lock().await;
        if let Some(client) = cached.as_ref() {
            return Ok(client.clone());
        }

        let scheme = if self.tls_ca_pem.is_some() {
            "https"
        } else {
            "http"
        };
        let mut endpoint = Endpoint::from_shared(format!("{scheme}://{}", self.addr))
            .map_err(|e| PeerError::Unreachable(format!("peer address {}: {e}", self.addr)))?
            .connect_timeout(PEER_DIAL_TIMEOUT)
            .tcp_nodelay(true)
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_timeout(Duration::from_secs(10))
            .keep_alive_while_idle(true);

        if let Some(ca) = &self.tls_ca_pem {
            let host = self
                .addr
                .rsplit_once(':')
                .map(|(h, _)| h)
                .unwrap_or(&self.addr);
            let tls = ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(ca.clone()))
                .domain_name(host.trim_matches(['[', ']']));
            endpoint = endpoint
                .tls_config(tls)
                .map_err(|e| PeerError::Unreachable(format!("peer TLS config: {e}")))?;
        }

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| PeerError::Unreachable(format!("dialing {}: {e}", self.addr)))?;

        let client = RaftPeerServiceClient::new(channel);
        *cached = Some(client.clone());
        Ok(client)
    }

    fn request<T>(&self, message: T, opts: &CallOpts) -> Request<T> {
        let mut req = Request::new(message);
        if let Some(timeout) = opts.timeout {
            req.set_timeout(timeout);
        }

        let md = req.metadata_mut();
        if let Some(key) = &self.send_key {
            md.insert("authorization", key.clone());
        }
        if let Some(cluster_id) = self.local.cluster_id() {
            md.insert(
                METADATA_CLUSTER_ID,
                cluster_id.to_string().parse().expect("uuid is ascii"),
            );
        }

        md.insert(
            METADATA_NODE_ID,
            self.local
                .node_id
                .to_string()
                .parse()
                .expect("digits are ascii"),
        );
        md.insert(
            METADATA_INSTANCE_UUID,
            self.local
                .instance_uuid
                .to_string()
                .parse()
                .expect("uuid is ascii"),
        );

        req
    }

    fn peer_error(&self, status: Status) -> PeerError {
        let classified = status
            .metadata()
            .get(METADATA_PEER_ERROR)
            .and_then(|v| v.to_str().ok());

        match classified {
            Some(PEER_ERROR_AUTH) => PeerError::AuthFailed(status.message().into()),
            Some(_) => PeerError::Refused(status.message().into()),
            None if status.code() == Code::Unauthenticated => {
                PeerError::AuthFailed(status.message().into())
            }
            None => match status.code() {
                Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled | Code::Unknown => {
                    PeerError::Unreachable(format!("{}: {}", self.addr, status.message()))
                }
                _ => PeerError::Unexpected(status.message().into()),
            },
        }
    }
}

impl PeerClient for TonicPeerClient {
    async fn handshake(
        &self,
        hello: pb::PeerHello,
        opts: CallOpts,
    ) -> Result<pb::PeerHello, PeerError> {
        let mut client = self.conn().await?;
        let req = self.request(pb::HandshakeRequest { hello: Some(hello) }, &opts);
        let resp = client
            .handshake(req)
            .await
            .map_err(|s| self.peer_error(s))?
            .into_inner();

        Ok(resp.hello.unwrap_or_default())
    }

    async fn append_entries(
        &self,
        req: pb::AppendEntriesRequest,
        opts: CallOpts,
    ) -> Result<pb::AppendEntriesResponse, PeerError> {
        let mut client = self.conn().await?;
        let req = self.request(req, &opts);

        Ok(client
            .append_entries(req)
            .await
            .map_err(|s| self.peer_error(s))?
            .into_inner())
    }

    async fn vote(
        &self,
        req: pb::VoteRequest,
        opts: CallOpts,
    ) -> Result<pb::VoteResponse, PeerError> {
        let mut client = self.conn().await?;
        let req = self.request(req, &opts);

        Ok(client
            .vote(req)
            .await
            .map_err(|s| self.peer_error(s))?
            .into_inner())
    }

    async fn install_snapshot(
        &self,
        start: pb::InstallSnapshotStart,
        file: PathBuf,
    ) -> Result<pb::Vote, PeerError> {
        let mut client = self.conn().await?;

        let (tx, rx) = tokio::sync::mpsc::channel::<pb::InstallSnapshotRequest>(4);
        let reader = tokio::spawn(async move {
            let mut src = match tokio::fs::File::open(&file).await {
                Ok(f) => f,
                Err(e) => {
                    warn!(path = %file.display(), error = %e, "snapshot file unreadable mid-send");
                    return;
                }
            };

            let start = pb::InstallSnapshotRequest {
                chunk: Some(pb::install_snapshot_request::Chunk::Start(start)),
            };
            if tx.send(start).await.is_err() {
                return;
            }

            loop {
                let mut buf = vec![0u8; SNAPSHOT_CHUNK_BYTES];
                let n = match src.read(&mut buf).await {
                    Ok(0) => return,
                    Ok(n) => n,
                    Err(e) => {
                        // Ending the stream early leaves the receiver a torn
                        // file its checksum verification rejects.
                        warn!(path = %file.display(), error = %e, "snapshot read failed mid-send");
                        return;
                    }
                };

                buf.truncate(n);
                let frame = pb::InstallSnapshotRequest {
                    chunk: Some(pb::install_snapshot_request::Chunk::Data(buf)),
                };

                if tx.send(frame).await.is_err() {
                    return;
                }
            }
        });

        let req = self.request(ReceiverStream::new(rx), &CallOpts::default());
        let result = client.install_snapshot(req).await;
        reader.abort();

        let resp = result.map_err(|s| self.peer_error(s))?.into_inner();
        resp.vote
            .ok_or_else(|| PeerError::Unexpected("snapshot response without vote".into()))
    }

    async fn trigger_elect(&self, opts: CallOpts) -> Result<(), PeerError> {
        let mut client = self.conn().await?;
        let req = self.request(pb::TriggerElectRequest {}, &opts);
        client
            .trigger_elect(req)
            .await
            .map_err(|s| self.peer_error(s))?;

        Ok(())
    }
}

// peer_auth_keys enforcement. All configured keys are accepted so rotation
// can roll through the cluster.
#[derive(Clone)]
pub struct PeerAuth {
    // None = auth disabled. An empty set rejects everyone.
    accepted: Option<HashSet<String>>,
}

impl PeerAuth {
    pub fn new(peer_auth_keys: &Option<Vec<String>>) -> Self {
        Self {
            accepted: peer_auth_keys
                .as_ref()
                .map(|keys| keys.iter().cloned().collect()),
        }
    }

    fn check(&self, metadata: &MetadataMap) -> Result<(), Status> {
        let Some(accepted) = &self.accepted else {
            return Ok(());
        };

        let presented = metadata
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));

        match presented {
            Some(key) if accepted.contains(key) => Ok(()),
            _ => {
                let mut status = Status::unauthenticated(
                    "peer auth failed: the presented key is not in this node's \
                     cluster.peer_auth_keys",
                );

                status
                    .metadata_mut()
                    .insert(METADATA_PEER_ERROR, PEER_ERROR_AUTH.parse().expect("ascii"));
                Err(status)
            }
        }
    }
}

fn extract_peer_meta(metadata: &MetadataMap) -> Result<PeerMeta, Status> {
    fn parsed<T: std::str::FromStr>(
        metadata: &MetadataMap,
        key: &'static str,
    ) -> Result<Option<T>, Status> {
        metadata
            .get(key)
            .map(|v| {
                v.to_str()
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| Status::invalid_argument(format!("malformed {key} metadata")))
            })
            .transpose()
    }

    Ok(PeerMeta {
        cluster_id: parsed::<Uuid>(metadata, METADATA_CLUSTER_ID)?,
        node_id: parsed::<u64>(metadata, METADATA_NODE_ID)?,
        instance_uuid: parsed::<Uuid>(metadata, METADATA_INSTANCE_UUID)?,
    })
}

pub struct PeerServer {
    receiver: std::sync::Arc<PeerReceiver>,
    auth: PeerAuth,
}

impl PeerServer {
    pub fn new(receiver: std::sync::Arc<PeerReceiver>, auth: PeerAuth) -> Self {
        Self { receiver, auth }
    }

    fn admit(&self, metadata: &MetadataMap) -> Result<PeerMeta, Status> {
        self.auth.check(metadata)?;
        extract_peer_meta(metadata)
    }
}

#[tonic::async_trait]
impl RaftPeerService for PeerServer {
    async fn handshake(
        &self,
        request: Request<pb::HandshakeRequest>,
    ) -> Result<Response<pb::HandshakeResponse>, Status> {
        let meta = self.admit(request.metadata())?;
        let resp = self.receiver.handshake(&meta, request.into_inner()).await?;

        Ok(Response::new(resp))
    }

    async fn append_entries(
        &self,
        request: Request<pb::AppendEntriesRequest>,
    ) -> Result<Response<pb::AppendEntriesResponse>, Status> {
        let meta = self.admit(request.metadata())?;
        let resp = self
            .receiver
            .append_entries(&meta, request.into_inner())
            .await?;

        Ok(Response::new(resp))
    }

    async fn vote(
        &self,
        request: Request<pb::VoteRequest>,
    ) -> Result<Response<pb::VoteResponse>, Status> {
        let meta = self.admit(request.metadata())?;
        let resp = self.receiver.vote(&meta, request.into_inner()).await?;

        Ok(Response::new(resp))
    }

    async fn install_snapshot(
        &self,
        request: Request<Streaming<pb::InstallSnapshotRequest>>,
    ) -> Result<Response<pb::InstallSnapshotResponse>, Status> {
        use pb::install_snapshot_request::Chunk;

        let meta = self.admit(request.metadata())?;
        // Refuse before accepting the body; the guard runs again inside
        // install_snapshot, which is harmless.
        self.receiver.check_raft(&meta)?;

        let mut stream = request.into_inner();
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("empty snapshot stream"))?;
        let Some(Chunk::Start(start)) = first.chunk else {
            return Err(Status::invalid_argument(
                "snapshot stream must open with a Start frame",
            ));
        };

        let file = self.receiver.begin_snapshot().await?;
        let io_err = |e: std::io::Error| Status::internal(format!("writing snapshot: {e}"));
        let mut out = tokio::fs::File::create(&file.path).await.map_err(io_err)?;
        while let Some(frame) = stream.message().await? {
            match frame.chunk {
                Some(Chunk::Data(bytes)) => out.write_all(&bytes).await.map_err(io_err)?,
                Some(Chunk::Start(_)) => {
                    return Err(Status::invalid_argument("duplicate Start frame"));
                }
                None => return Err(Status::invalid_argument("empty snapshot frame")),
            }
        }
        out.sync_all().await.map_err(io_err)?;
        drop(out);

        let vote = self.receiver.install_snapshot(&meta, start, file).await?;
        Ok(Response::new(pb::InstallSnapshotResponse {
            vote: Some(vote),
        }))
    }

    async fn trigger_elect(
        &self,
        request: Request<pb::TriggerElectRequest>,
    ) -> Result<Response<pb::TriggerElectResponse>, Status> {
        let meta = self.admit(request.metadata())?;
        self.receiver.trigger_elect(&meta).await?;

        Ok(Response::new(pb::TriggerElectResponse {}))
    }
}

pub struct PeerListener {
    pub addr: SocketAddr,
    pub handle: tokio::task::JoinHandle<()>,
}

pub async fn spawn_peer_listener(
    cluster: &ClusterConfig,
    max_message_bytes: u64,
    server: PeerServer,
    mut shutdown: watch::Receiver<bool>,
) -> Result<PeerListener, Box<dyn std::error::Error>> {
    let mut builder = Server::builder()
        .http2_keepalive_interval(Some(Duration::from_secs(30)))
        .http2_keepalive_timeout(Some(Duration::from_secs(10)));

    if let (Some(cert_path), Some(key_path)) = (
        cluster.peer_tls_cert_path.as_deref(),
        cluster.peer_tls_key_path.as_deref(),
    ) {
        let cert = std::fs::read(cert_path)
            .map_err(|e| format!("reading cluster.peer_tls_cert_path ({cert_path}): {e}"))?;
        let key = std::fs::read(key_path)
            .map_err(|e| format!("reading cluster.peer_tls_key_path ({key_path}): {e}"))?;
        builder =
            builder.tls_config(ServerTlsConfig::new().identity(Identity::from_pem(cert, key)))?;
    }

    let incoming = TcpIncoming::bind(cluster.peer_listen_addr)
        .map_err(|e| format!("binding {}: {e}", cluster.peer_listen_addr))?
        .with_nodelay(Some(true));
    let addr = incoming
        .local_addr()
        .map_err(|e| format!("resolving bound peer address: {e}"))?;

    let service = RaftPeerServiceServer::new(server)
        .max_decoding_message_size(frame_cap(max_message_bytes) as usize);

    let handle = tokio::spawn(async move {
        let result = builder
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown.wait_for(|stop| *stop).await;
            })
            .await;
        if let Err(e) = result {
            error!(error = %e, "peer listener failed");
        }
    });

    info!(addr = %addr, "peer listener listening");
    Ok(PeerListener { addr, handle })
}
