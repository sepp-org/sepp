// Snapshot files and the fenced install protocol.
//
// A snapshot is a checksummed file of length-prefixed proto frames: a Header
// (format version + SnapshotMeta), then per state machine keyspace a
// Keyspace frame followed by its Kv frames in ascending key order, then a
// Trailer whose SHA-256 covers every preceding file byte. Files are built
// from an MVCC read snapshot without stopping writes and double as the
// node's current snapshot across restarts.
//
// Install replaces the state machine by delete + recreate + ingest, fenced
// by a durable marker in the `raft` keyspace: fjall ingestion bypasses the
// journal, so journal recovery cannot protect a half-install, and the
// engine purges the log concurrently with the install, so a node that
// crashes mid-install has no self-repair except finishing the job. Boot
// therefore rolls a marked install forward from the retained file, never
// back. `clear()` is never used: journal replay re-executes Clear markers
// over ingested data (fjall 3.1.8, reproduced); deletion gives the
// recreated keyspaces fresh internal ids that make stale journal records
// unresolvable on replay.

use std::fs::{self, File};
use std::io::{BufReader, BufWriter, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};

use fjall::{
    KeyspaceCreateOptions, PersistMode, Readable, SingleWriterTxDatabase as TxDatabase,
    SingleWriterTxKeyspace as TxKeyspace,
};
use prost::Message;
use sha2::{Digest as _, Sha256};
use tracing::{info, warn};

use super::{LogId, SnapshotMeta, log_id_from_proto, snapshot_meta_from_proto};
use crate::keys::{LAST_APPLIED_KEY, MEMBERSHIP_KEY};
use crate::pb::sepp::raft::v1 as pb;
use crate::storage::{Keyspaces, SM_KEYSPACES, now_ms};

pub(crate) type SnapshotError = Box<dyn std::error::Error + Send + Sync>;

const SNAPSHOT_FORMAT_VERSION: u32 = 1;
const SNAPSHOT_INSTALLING_KEY: &[u8] = b"snapshot_installing";

// One frame must fit in memory on both ends.
const FRAME_CAP_FLOOR_BYTES: u32 = 64 * 1024 * 1024;
const FRAME_SLACK_BYTES: u64 = 1024 * 1024;

// The largest limits.max_message_bytes cluster mode accepts: one message
// plus slack must stay u32-frame-encodable.
pub(crate) const MAX_CLUSTER_MESSAGE_BYTES: u64 = u32::MAX as u64 - FRAME_SLACK_BYTES;

pub(crate) fn frame_cap(max_message_bytes: u64) -> u32 {
    max_message_bytes
        .saturating_add(FRAME_SLACK_BYTES)
        .clamp(u64::from(FRAME_CAP_FLOOR_BYTES), u64::from(u32::MAX)) as u32
}

// The snapshot data type the engine moves between builder, sender and
// installer. Self-describing: the file's header carries the SnapshotMeta.
#[derive(Debug)]
pub struct SnapshotFile {
    pub path: PathBuf,
}

// The node's snapshot directory, inside the fjall directory (whose recovery
// walks only its own subfolders) so one volume snapshot stays
// self-consistent.
#[derive(Clone)]
pub(crate) struct SnapshotDir {
    dir: PathBuf,
    frame_cap: u32,
}

impl SnapshotDir {
    pub(crate) fn open(db_path: &str, frame_cap: u32) -> std::io::Result<Self> {
        let dir = Path::new(db_path).join("snapshots");
        fs::create_dir_all(&dir)?;
        Ok(Self { dir, frame_cap })
    }

    pub(crate) fn frame_cap(&self) -> u32 {
        self.frame_cap
    }

    pub(crate) fn file_path(&self, snapshot_id: &str) -> PathBuf {
        self.dir.join(format!("{snapshot_id}.snap"))
    }

    // Where an install parks the received file until the state machine
    // durably contains it. The `.staged` extension keeps it out of
    // `current()`: a node must never advertise a snapshot ahead of its own
    // applied state (openraft validates snapshot <= committed at boot, and
    // a leader-side stale advert wedges snapshot replication).
    pub(crate) fn staged_path(&self, snapshot_id: &str) -> PathBuf {
        self.dir.join(format!("{snapshot_id}.snap.staged"))
    }

    pub(crate) fn incoming_path(&self, seq: u64) -> PathBuf {
        self.dir.join(format!("incoming-{seq}.partial"))
    }

    pub(crate) fn contains(&self, path: &Path) -> bool {
        path.parent() == Some(self.dir.as_path())
    }

    // The current snapshot: the readable `.snap` file covering the highest
    // log id. Unreadable files are skipped, not fatal: a lost or torn
    // current snapshot is rebuilt by the engine via the snapshot builder.
    pub(crate) fn current(&self) -> std::io::Result<Option<(SnapshotMeta, PathBuf)>> {
        let mut best: Option<(SnapshotMeta, PathBuf)> = None;
        for dirent in fs::read_dir(&self.dir)? {
            let path = dirent?.path();
            if path.extension().is_none_or(|ext| ext != "snap") {
                continue;
            }

            let meta = match read_snapshot_header(&path, self.frame_cap) {
                Ok(meta) => meta,
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "skipping unreadable snapshot file");
                    continue;
                }
            };

            if best
                .as_ref()
                .is_none_or(|(b, _)| b.last_log_id < meta.last_log_id)
            {
                best = Some((meta, path));
            }
        }
        Ok(best)
    }

    // Older snapshots are covered by `keep` (higher last log id), so nothing
    // the leader still serves depends on them.
    pub(crate) fn remove_others(&self, keep: &Path) {
        let Ok(entries) = fs::read_dir(&self.dir) else {
            return;
        };

        for path in entries.flatten().map(|d| d.path()) {
            if path != keep && path.extension().is_some_and(|ext| ext == "snap") {
                let _ = fs::remove_file(&path);
            }
        }
    }
}

fn mint_snapshot_id(last: Option<&LogId>) -> String {
    match last {
        Some(l) => format!(
            "{}-{}-{}-{}",
            l.leader_id.term,
            l.leader_id.node_id,
            l.index,
            now_ms()
        ),
        None => format!("0-0-0-{}", now_ms()),
    }
}

struct HashingWriter<W: Write> {
    inner: W,
    hasher: Sha256,
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn write_frame(
    w: &mut impl Write,
    frame: pb::snapshot_frame::Frame,
    frame_cap: u32,
) -> std::io::Result<()> {
    let bytes = pb::SnapshotFrame { frame: Some(frame) }.encode_to_vec();
    if bytes.len() > frame_cap as usize {
        return Err(std::io::Error::other(format!(
            "snapshot frame of {} bytes exceeds the {frame_cap}-byte cap",
            bytes.len()
        )));
    }
    w.write_all(&(bytes.len() as u32).to_be_bytes())?;
    w.write_all(&bytes)
}

// Builds a snapshot of the current applied state into `dir` and returns its
// meta and path. Runs against an MVCC read snapshot, so writes continue.
pub(crate) fn build_snapshot_file(
    db: &TxDatabase,
    dir: &SnapshotDir,
) -> Result<(SnapshotMeta, PathBuf), SnapshotError> {
    // Fetched by name, not cached: a snapshot install swaps the keyspaces
    // out from under any long-lived handle.
    let ks = Keyspaces::open(db)?;
    let snap = db.read_tx();

    let last_log_id = snap
        .get(&ks.meta, LAST_APPLIED_KEY)?
        .map(|v| pb::LogId::decode(v.as_ref()))
        .transpose()?
        .map(|msg| log_id_from_proto(&msg));
    let last_membership = snap
        .get(&ks.meta, MEMBERSHIP_KEY)?
        .map(|v| pb::StoredMembership::decode(v.as_ref()))
        .transpose()?
        .map(super::stored_membership_from_proto)
        .unwrap_or_default();
    let meta = SnapshotMeta {
        last_log_id,
        last_membership,
        snapshot_id: mint_snapshot_id(last_log_id.as_ref()),
    };

    let path = dir.file_path(&meta.snapshot_id);
    let tmp = path.with_extension("tmp");
    let file = File::create(&tmp)?;
    let mut w = HashingWriter {
        inner: BufWriter::new(file),
        hasher: Sha256::new(),
    };

    write_frame(
        &mut w,
        pb::snapshot_frame::Frame::Header(pb::SnapshotHeader {
            format_version: SNAPSHOT_FORMAT_VERSION,
            meta: Some(super::snapshot_meta_to_proto(&meta)),
        }),
        dir.frame_cap,
    )?;

    for name in SM_KEYSPACES {
        let keyspace = ks.by_name(name).expect("SM_KEYSPACES names its fields");
        write_frame(
            &mut w,
            pb::snapshot_frame::Frame::Keyspace(pb::SnapshotKeyspace { name: name.into() }),
            dir.frame_cap,
        )?;

        for guard in snap.iter(keyspace) {
            let (key, value) = guard.into_inner()?;
            write_frame(
                &mut w,
                pb::snapshot_frame::Frame::Kv(pb::SnapshotKv {
                    key: key.to_vec(),
                    value: value.to_vec(),
                }),
                dir.frame_cap,
            )?;
        }
    }

    let sha256 = w.hasher.clone().finalize().to_vec();
    write_frame(
        &mut w,
        pb::snapshot_frame::Frame::Trailer(pb::SnapshotTrailer { sha256 }),
        dir.frame_cap,
    )?;

    let file = w.inner.into_inner().map_err(|e| e.into_error())?;
    file.sync_all()?;
    drop(file);

    // A published snapshot must never claim state the state machine can lose to a
    // crash, so make the claimed state durable before advertising the file.
    db.persist(PersistMode::SyncData)?;

    // Windows renames refuse an existing target; ids are timestamped, so a
    // collision only means an identical rebuild.
    let _ = fs::remove_file(&path);
    fs::rename(&tmp, &path)?;
    sync_dir(&dir.dir)?;
    dir.remove_others(&path);

    Ok((meta, path))
}

#[cfg(not(windows))]
fn sync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

// Windows cannot open directories as files; directory entries are made
// durable by the volume, not an explicit fsync. A rename lost to a crash
// loses only the current-snapshot pointer, which the engine rebuilds.
#[cfg(windows)]
fn sync_dir(_dir: &Path) -> std::io::Result<()> {
    Ok(())
}

enum SnapshotEvent {
    Section(String),
    Kv(Vec<u8>, Vec<u8>),
}

// A pull reader over a snapshot file that enforces the full format contract
// as it goes: header first, the exact SM keyspace set in canonical order,
// strictly ascending keys per section, then a checksum-matching trailer
// with nothing after it. `next` returns None only for a fully valid file.
struct SnapshotStream {
    reader: BufReader<File>,
    hasher: Sha256,
    meta: SnapshotMeta,
    sections: Vec<String>,
    last_key: Option<Vec<u8>>,
    done: bool,
    frame_cap: u32,
}

impl SnapshotStream {
    fn open(path: &Path, frame_cap: u32) -> Result<Self, SnapshotError> {
        let mut stream = Self {
            reader: BufReader::new(File::open(path)?),
            hasher: Sha256::new(),
            meta: SnapshotMeta::default(),
            sections: Vec::new(),
            last_key: None,
            done: false,
            frame_cap,
        };

        match stream.read_frame()? {
            pb::snapshot_frame::Frame::Header(h) => {
                if h.format_version != SNAPSHOT_FORMAT_VERSION {
                    return Err(format!(
                        "snapshot format {} is not supported (this binary reads {})",
                        h.format_version, SNAPSHOT_FORMAT_VERSION
                    )
                    .into());
                }
                stream.meta =
                    snapshot_meta_from_proto(h.meta.ok_or("snapshot header without meta")?);
            }
            _ => return Err("snapshot file does not start with a header".into()),
        }

        Ok(stream)
    }

    // Reads one length-prefixed frame, folding every pre-trailer byte into
    // the running checksum.
    fn read_frame(&mut self) -> Result<pb::snapshot_frame::Frame, SnapshotError> {
        let truncated = |e: std::io::Error| -> SnapshotError {
            if e.kind() == ErrorKind::UnexpectedEof {
                "snapshot file is truncated".into()
            } else {
                e.into()
            }
        };

        let mut len_bytes = [0u8; 4];
        self.reader.read_exact(&mut len_bytes).map_err(truncated)?;
        let len = u32::from_be_bytes(len_bytes);
        if len > self.frame_cap {
            return Err(format!(
                "snapshot frame of {len} bytes exceeds this node's cap of {} (is \
                 limits.max_message_bytes lower than the builder's?)",
                self.frame_cap
            )
            .into());
        }

        let mut frame_bytes = vec![0u8; len as usize];
        self.reader
            .read_exact(&mut frame_bytes)
            .map_err(truncated)?;

        let frame = pb::SnapshotFrame::decode(frame_bytes.as_slice())?
            .frame
            .ok_or("empty snapshot frame")?;
        if !matches!(frame, pb::snapshot_frame::Frame::Trailer(_)) {
            self.hasher.update(len_bytes);
            self.hasher.update(&frame_bytes);
        }

        Ok(frame)
    }

    fn next(&mut self) -> Result<Option<SnapshotEvent>, SnapshotError> {
        if self.done {
            return Ok(None);
        }

        match self.read_frame()? {
            pb::snapshot_frame::Frame::Header(_) => Err("snapshot file has two headers".into()),
            pb::snapshot_frame::Frame::Keyspace(section) => {
                self.sections.push(section.name.clone());
                self.last_key = None;
                Ok(Some(SnapshotEvent::Section(section.name)))
            }
            pb::snapshot_frame::Frame::Kv(kv) => {
                if self.sections.is_empty() {
                    return Err("snapshot kv frame before any keyspace frame".into());
                }
                // Ingestion requires ascending keys; fail cleanly here, not
                // in fjall's panic.
                if self
                    .last_key
                    .as_deref()
                    .is_some_and(|last| last >= &kv.key[..])
                {
                    return Err(format!(
                        "snapshot keys out of order in {:?}",
                        self.sections.last()
                    )
                    .into());
                }

                self.last_key = Some(kv.key.clone());
                Ok(Some(SnapshotEvent::Kv(kv.key, kv.value)))
            }
            pb::snapshot_frame::Frame::Trailer(trailer) => {
                let computed: [u8; 32] = self.hasher.clone().finalize().into();
                if computed.as_slice() != trailer.sha256.as_slice() {
                    return Err("snapshot checksum mismatch".into());
                }

                if self.reader.read(&mut [0u8; 1])? != 0 {
                    return Err("snapshot file has data after the trailer".into());
                }

                if self.sections != SM_KEYSPACES {
                    return Err(format!(
                        "snapshot keyspaces {:?} do not match the state machine set",
                        self.sections
                    )
                    .into());
                }

                self.done = true;
                Ok(None)
            }
        }
    }
}

fn read_snapshot_header(path: &Path, frame_cap: u32) -> Result<SnapshotMeta, SnapshotError> {
    Ok(SnapshotStream::open(path, frame_cap)?.meta)
}

pub(crate) fn verify_snapshot_file(
    path: &Path,
    frame_cap: u32,
) -> Result<SnapshotMeta, SnapshotError> {
    let mut stream = SnapshotStream::open(path, frame_cap)?;
    while stream.next()?.is_some() {}

    Ok(stream.meta)
}

// The destructive install step: delete + recreate every state machine
// keyspace, then ingest the file's sorted streams. Idempotent (delete is
// delete-if-exists), so a crash anywhere inside is redone wholesale from
// the marker at boot. Returns the fresh handles for the Store to swap in.
pub(crate) fn ingest_snapshot_file(
    db: &TxDatabase,
    path: &Path,
    frame_cap: u32,
) -> Result<Keyspaces, SnapshotError> {
    let mut stream = SnapshotStream::open(path, frame_cap)?;

    // Delete before recreate: the fresh internal ids make stale journal
    // records for the old keyspaces unresolvable on replay.
    for name in SM_KEYSPACES {
        let handle = db.keyspace(name, KeyspaceCreateOptions::default)?;
        db.inner().delete_keyspace(handle.inner().clone())?;
    }
    let keyspaces = Keyspaces::open(db)?;

    // fjall's Ingestion is unnameable outside the crate, so each section's
    // ingestion lives in an inferred-type scope: entered on the section's
    // first kv, finished when the section ends.
    let mut pending = stream.next()?;
    while let Some(event) = pending.take() {
        let SnapshotEvent::Section(name) = event else {
            return Err("snapshot kv frame outside a keyspace section".into());
        };
        let keyspace = keyspaces
            .by_name(&name)
            .ok_or_else(|| format!("snapshot names unknown keyspace {name:?}"))?;

        let mut next = stream.next()?;
        if matches!(next, Some(SnapshotEvent::Kv(..))) {
            let mut ingestion = keyspace.inner().start_ingestion()?;
            while let Some(SnapshotEvent::Kv(key, value)) = next {
                ingestion.write(key, value)?;
                next = stream.next()?;
            }

            // Each finish is independently durable: fsynced tables plus the
            // version manifest.
            ingestion.finish()?;
        }

        pending = next;
    }

    Ok(keyspaces)
}

pub(crate) fn read_install_marker(
    db: &TxDatabase,
    raft: &TxKeyspace,
) -> Result<Option<pb::SnapshotInstallMarker>, SnapshotError> {
    let Some(bytes) = db.read_tx().get(raft, SNAPSHOT_INSTALLING_KEY)? else {
        return Ok(None);
    };

    Ok(Some(pb::SnapshotInstallMarker::decode(bytes.as_ref())?))
}

// The fence: durable before the engine is told about the snapshot, because
// the engine purges the log concurrently with the install. From this point
// the install must complete, in this process or at the next boot.
pub(crate) fn write_install_marker(
    db: &TxDatabase,
    raft: &TxKeyspace,
    meta: &SnapshotMeta,
    file_name: &str,
) -> Result<(), fjall::Error> {
    let marker = pb::SnapshotInstallMarker {
        meta: Some(super::snapshot_meta_to_proto(meta)),
        file_name: file_name.into(),
    };

    let mut tx = db.write_tx();
    tx.insert(
        raft,
        SNAPSHOT_INSTALLING_KEY.to_vec(),
        marker.encode_to_vec(),
    );
    tx.commit()?;
    db.persist(PersistMode::SyncAll)
}

pub(crate) fn clear_install_marker(db: &TxDatabase, raft: &TxKeyspace) -> Result<(), fjall::Error> {
    let mut tx = db.write_tx();

    tx.remove(raft, SNAPSHOT_INSTALLING_KEY.to_vec());
    tx.commit()?;
    db.persist(PersistMode::SyncAll)
}

// Refuses to boot over an unfinished install when cluster mode is off: the
// state machine keyspaces may be half-destroyed, and silently recreating
// them empty would present a wrecked store as a healthy new one. Reads the
// marker only if the `raft` keyspace already exists, so plain standalone
// databases are never touched.
pub fn refuse_pending_install(
    db: &TxDatabase,
    db_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if !db.keyspace_exists("raft") {
        return Ok(());
    }

    let raft = db.keyspace("raft", KeyspaceCreateOptions::default)?;
    match read_install_marker(db, &raft) {
        Ok(None) => Ok(()),
        Ok(Some(_)) => Err(format!(
            "refusing to open database at {db_path:?}: it holds an unfinished cluster \
             snapshot install; enable [cluster] and restart to finish it, or wipe the data dir",
        )
        .into()),
        Err(e) => Err(format!(
            "refusing to open database at {db_path:?}: its snapshot install marker is \
             unreadable ({e}); wipe the data dir and re-add this node",
        )
        .into()),
    }
}

// Boot-time roll-forward: a present marker means an install did not finish.
// Redo it from the retained file before anything reads the state machine
// keyspaces. Never rolls back: the snapshot is committed state and the
// local log may already be purged below it.
pub fn roll_forward_pending_install(
    db: &TxDatabase,
    db_path: &str,
    max_message_bytes: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    // Return-position coercion drops the Send + Sync markers `?` cannot.
    fn erase(e: SnapshotError) -> Box<dyn std::error::Error> {
        e
    }

    let raft = db.keyspace("raft", KeyspaceCreateOptions::default)?;
    let Some(marker) = read_install_marker(db, &raft).map_err(erase)? else {
        return Ok(());
    };

    let dir = SnapshotDir::open(db_path, frame_cap(max_message_bytes))?;
    let final_path = dir.dir.join(&marker.file_name);
    let staged = dir.dir.join(format!("{}.staged", marker.file_name));
    let source = if final_path.exists() {
        final_path.clone()
    } else {
        staged.clone()
    };

    verify_snapshot_file(&source, dir.frame_cap).map_err(|e| {
        format!(
            "boot found an unfinished snapshot install and its file {:?} is unusable ({e}); \
             wipe the data dir and re-add this node",
            marker.file_name,
        )
    })?;

    info!(file = %source.display(), "rolling an unfinished snapshot install forward");
    ingest_snapshot_file(db, &source, dir.frame_cap).map_err(erase)?;

    if source == staged {
        let _ = fs::remove_file(&final_path);
        fs::rename(&staged, &final_path)?;
        sync_dir(&dir.dir)?;
    }
    clear_install_marker(db, &raft)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, SeekFrom};

    use openraft::{EntryPayload, LeaderId};
    use uuid::Uuid;

    use super::*;
    use crate::config::Config;
    use crate::metrics::Metrics;
    use crate::op::{JobLimits, Op, PreparedJob};
    use crate::pb::sepp::v1::EnqueueRequest;
    use crate::queues::QueueRegistry;
    use crate::storage::{
        ApplyCore, QueueNotifiers, StampClamp, StorageParams, Store, logical_contents,
        rebuild_indexes,
    };

    fn temp_path() -> PathBuf {
        std::env::temp_dir().join(format!("sepp-snapshot-test-{}", Uuid::new_v4()))
    }

    fn cap() -> u32 {
        frame_cap(16 << 20)
    }

    fn open_core(db: &TxDatabase) -> ApplyCore {
        let store = Store::new(
            db.clone(),
            Keyspaces::open(db).expect("open keyspaces"),
            StorageParams {
                persist_mode: PersistMode::Buffer,
                sweep_limit: 100,
                dead_letter_retention_ms: 60_000,
                admin_enabled: false,
            },
            Metrics::new(false),
        );
        let indexes = rebuild_indexes(&store).expect("rebuild");
        ApplyCore::new(
            store,
            indexes,
            QueueNotifiers::default(),
            StampClamp::new(0),
        )
        .expect("apply core")
    }

    fn job(id: &str, queue: &str) -> PreparedJob {
        let registry = QueueRegistry::from_config(&Config::default());
        let req = EnqueueRequest {
            queue: queue.into(),
            job_type: "t".into(),
            ..Default::default()
        };
        PreparedJob {
            id: id.into(),
            limits: JobLimits::resolve(&req, &registry, &registry),
            req,
        }
    }

    fn entry(index: u64, op: Op) -> crate::raft::Entry {
        crate::raft::Entry {
            log_id: LogId {
                leader_id: LeaderId::new(1, 1),
                index,
            },
            payload: EntryPayload::Normal(op),
        }
    }

    // Applies a small but multi-keyspace state through the raft path, so the
    // meta rows (last_applied, digest) are populated like a real follower's.
    fn populate(core: &mut ApplyCore) {
        let t = 1_000_000;
        let ops = vec![
            Op::Enqueue {
                jobs: vec![job("j1", "alpha"), job("j2", "beta")],
                now_ms: t,
            },
            Op::Reserve {
                queues: vec!["alpha".into()],
                lease_ms: 10_000,
                max_jobs: 1,
                now_ms: t + 100,
            },
            Op::AuditAppend {
                record: crate::pb::sepp::storage::v1::AuditRecord {
                    actor: "root".into(),
                    role: "admin".into(),
                    action: "test".into(),
                    details_json: "{}".into(),
                },
                now_ms: t + 200,
            },
        ];
        for (i, op) in ops.into_iter().enumerate() {
            core.apply_entries(vec![entry(i as u64 + 1, op)]);
        }
    }

    #[test]
    fn build_verify_and_reingest_round_trip() {
        let source_path = temp_path();
        let db = TxDatabase::builder(&source_path)
            .temporary(true)
            .open()
            .expect("open db");
        let mut core = open_core(&db);
        populate(&mut core);
        let source_contents = logical_contents(core.store());

        let dir = SnapshotDir::open(source_path.to_str().unwrap(), cap()).expect("snapshot dir");
        let (meta, path) = build_snapshot_file(&db, &dir).expect("build");
        assert_eq!(
            meta.last_log_id.map(|l| l.index),
            Some(3),
            "snapshot meta carries the applied log id"
        );

        assert_eq!(
            verify_snapshot_file(&path, cap())
                .expect("verify")
                .snapshot_id,
            meta.snapshot_id
        );
        let (current_meta, current_path) = dir.current().expect("scan").expect("current");
        assert_eq!(current_meta.snapshot_id, meta.snapshot_id);
        assert_eq!(current_path, path);

        // Install into a fresh database and compare every keyspace byte,
        // meta rows included: the stream self-describes.
        let dest_path = temp_path();
        let dest = TxDatabase::builder(&dest_path)
            .temporary(true)
            .open()
            .expect("open dest");
        let mut doomed = open_core(&dest);
        doomed.apply_entries(vec![entry(
            1,
            Op::Enqueue {
                jobs: vec![job("doomed", "gamma")],
                now_ms: 2_000_000,
            },
        )]);
        drop(doomed);

        let keyspaces = ingest_snapshot_file(&dest, &path, cap()).expect("ingest");
        let installed = Store::new(
            dest.clone(),
            keyspaces,
            StorageParams {
                persist_mode: PersistMode::Buffer,
                sweep_limit: 100,
                dead_letter_retention_ms: 60_000,
                admin_enabled: false,
            },
            Metrics::new(false),
        );
        assert_eq!(
            logical_contents(&installed),
            source_contents,
            "installed state must be byte-identical to the source"
        );
        rebuild_indexes(&installed).expect("the existing boot path runs after install");
    }

    #[test]
    fn verification_rejects_corruption_and_truncation() {
        let path = temp_path();
        let db = TxDatabase::builder(&path)
            .temporary(true)
            .open()
            .expect("open db");
        let mut core = open_core(&db);
        populate(&mut core);
        let dir = SnapshotDir::open(path.to_str().unwrap(), cap()).expect("snapshot dir");
        let (_, file) = build_snapshot_file(&db, &dir).expect("build");

        // Flip one byte mid-file.
        let corrupt = file.with_extension("corrupt.snap");
        fs::copy(&file, &corrupt).expect("copy");
        {
            let mut f = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&corrupt)
                .expect("open");
            let len = f.metadata().expect("meta").len();
            f.seek(SeekFrom::Start(len / 2)).expect("seek");
            let mut byte = [0u8; 1];
            f.read_exact(&mut byte).expect("read");
            f.seek(SeekFrom::Start(len / 2)).expect("seek");
            f.write_all(&[byte[0] ^ 0xff]).expect("write");
        }
        // The exact failure depends on where the flip lands (checksum,
        // framing or decode); rejection is the contract.
        verify_snapshot_file(&corrupt, cap()).expect_err("corruption must fail");

        // Cut the trailer off.
        let truncated = file.with_extension("truncated.snap");
        fs::copy(&file, &truncated).expect("copy");
        let len = fs::metadata(&truncated).expect("meta").len();
        let f = fs::OpenOptions::new()
            .write(true)
            .open(&truncated)
            .expect("open");
        f.set_len(len - 20).expect("truncate");
        drop(f);
        let err = verify_snapshot_file(&truncated, cap()).expect_err("truncation must fail");
        assert!(
            format!("{err}").contains("truncated"),
            "unexpected error: {err}"
        );
    }

    // The crash-mid-install gate: marker written, process dies anywhere in
    // the destructive window, boot rolls the install forward from the file.
    #[test]
    fn boot_rolls_a_marked_install_forward() {
        // Source state and its snapshot file.
        let source_path = temp_path();
        let source_db = TxDatabase::builder(&source_path)
            .temporary(true)
            .open()
            .expect("open source");
        let mut core = open_core(&source_db);
        populate(&mut core);
        let source_contents = logical_contents(core.store());
        let source_dir = SnapshotDir::open(source_path.to_str().unwrap(), cap()).expect("dir");
        let (meta, file) = build_snapshot_file(&source_db, &source_dir).expect("build");

        // The victim: different pre-install state, persistent so it can
        // "crash" and reopen.
        let victim_path = temp_path();
        let victim_str = victim_path.to_str().unwrap().to_string();
        {
            let db = TxDatabase::builder(&victim_path)
                .open()
                .expect("open victim");
            let mut doomed = open_core(&db);
            doomed.apply_entries(vec![entry(
                1,
                Op::Enqueue {
                    jobs: vec![job("doomed", "gamma")],
                    now_ms: 2_000_000,
                },
            )]);
            drop(doomed);

            // The install's steps up to the fence: file parked under its
            // STAGED name (never advertised), marker naming the final file,
            // durable. Crash before any ingest.
            let dir = SnapshotDir::open(&victim_str, cap()).expect("dir");
            fs::copy(&file, dir.staged_path(&meta.snapshot_id)).expect("stage file");
            let raft = db
                .keyspace("raft", KeyspaceCreateOptions::default)
                .expect("raft ks");
            write_install_marker(&db, &raft, &meta, &format!("{}.snap", meta.snapshot_id))
                .expect("marker");
        }

        // First reboot: marker present, nothing ingested yet, file still
        // under the staged name.
        {
            let db = TxDatabase::builder(&victim_path).open().expect("reopen");
            roll_forward_pending_install(&db, &victim_str, 16 << 20).expect("roll forward");
            let raft = db
                .keyspace("raft", KeyspaceCreateOptions::default)
                .expect("raft ks");
            assert!(
                read_install_marker(&db, &raft).expect("read").is_none(),
                "a finished install clears the marker"
            );

            // Roll-forward finishes the publish: the file now carries the
            // final name and is this node's current snapshot.
            let dir = SnapshotDir::open(&victim_str, cap()).expect("dir");
            assert!(!dir.staged_path(&meta.snapshot_id).exists());
            let (current, _) = dir.current().expect("scan").expect("current");
            assert_eq!(current.snapshot_id, meta.snapshot_id);

            // Crash again mid-redo: keyspaces half-destroyed, marker back.
            write_install_marker(&db, &raft, &meta, &format!("{}.snap", meta.snapshot_id))
                .expect("marker again");
            for name in ["jobs", "ready", "meta"] {
                let handle = db
                    .keyspace(name, KeyspaceCreateOptions::default)
                    .expect("ks");
                db.inner()
                    .delete_keyspace(handle.inner().clone())
                    .expect("delete");
            }
        }

        // Second reboot: marker present over half-destroyed keyspaces.
        {
            let db = TxDatabase::builder(&victim_path).open().expect("reopen 2");
            roll_forward_pending_install(&db, &victim_str, 16 << 20).expect("roll forward again");

            let store = Store::new(
                db.clone(),
                Keyspaces::open(&db).expect("keyspaces"),
                StorageParams {
                    persist_mode: PersistMode::Buffer,
                    sweep_limit: 100,
                    dead_letter_retention_ms: 60_000,
                    admin_enabled: false,
                },
                Metrics::new(false),
            );
            assert_eq!(
                logical_contents(&store),
                source_contents,
                "roll-forward must land exactly on the snapshot state"
            );
            rebuild_indexes(&store).expect("boot index rebuild");

            // And with no marker, roll-forward is a no-op.
            roll_forward_pending_install(&db, &victim_str, 16 << 20).expect("idempotent");
        }

        let _ = fs::remove_dir_all(&victim_path);
    }

    #[test]
    fn roll_forward_refuses_an_unusable_file() {
        let path = temp_path();
        let path_str = path.to_str().unwrap().to_string();
        let db = TxDatabase::builder(&path)
            .temporary(true)
            .open()
            .expect("open");
        let raft = db
            .keyspace("raft", KeyspaceCreateOptions::default)
            .expect("raft ks");
        SnapshotDir::open(&path_str, cap()).expect("dir");
        write_install_marker(&db, &raft, &SnapshotMeta::default(), "missing.snap").expect("marker");

        let err = roll_forward_pending_install(&db, &path_str, 16 << 20)
            .expect_err("a lost file leaves the node unrecoverable");
        assert!(
            err.to_string().contains("wipe the data dir"),
            "the error must name the fix: {err}"
        );
    }

    #[test]
    fn frame_cap_floors_and_tracks_the_config() {
        assert_eq!(frame_cap(0), FRAME_CAP_FLOOR_BYTES);
        assert_eq!(frame_cap(16 << 20), FRAME_CAP_FLOOR_BYTES);
        let big = 256u64 << 20;
        assert_eq!(u64::from(frame_cap(big)), big + FRAME_SLACK_BYTES);
        assert_eq!(frame_cap(MAX_CLUSTER_MESSAGE_BYTES), u32::MAX);
        assert_eq!(frame_cap(u64::MAX), u32::MAX);
    }

    #[test]
    fn caps_are_enforced_on_both_ends() {
        let path = temp_path();
        let db = TxDatabase::builder(&path)
            .temporary(true)
            .open()
            .expect("open db");
        let mut core = open_core(&db);
        populate(&mut core);

        // A writer refuses to produce a frame its own cap rejects.
        let tiny = SnapshotDir::open(path.to_str().unwrap(), 16).expect("dir");
        let err = build_snapshot_file(&db, &tiny).expect_err("tiny cap must fail the build");
        assert!(
            err.to_string().contains("exceeds"),
            "unexpected error: {err}"
        );

        // A reader with a smaller cap than the builder names the suspect.
        let dir = SnapshotDir::open(path.to_str().unwrap(), cap()).expect("dir");
        let (_, file) = build_snapshot_file(&db, &dir).expect("build");
        let err = verify_snapshot_file(&file, 8).expect_err("small cap must refuse");
        assert!(
            err.to_string().contains("max_message_bytes"),
            "the error must name the likely drift: {err}"
        );
    }
}
