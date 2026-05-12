use std::{
    collections::{HashMap, VecDeque},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_core::Stream;
use prost_protovalidate::Validator;
use tokio::{
    sync::{Mutex, Notify, mpsc},
    time::{Instant, sleep_until},
};
use tonic::{Request, Response, Status, Streaming, transport::Server};
use uuid::Uuid;

use crate::pb::sepp::v1::{
    AckRequest, AckResponse, BatchSuccess, EnqueueBatchRequest, EnqueueBatchResponse,
    EnqueueRequest, EnqueueResponse, ErrorDetails, ExtendRequest, ExtendResponse,
    GetServerInfoRequest, GetServerInfoResponse, Job, NackRequest, NackResponse, Payload,
    PrimitiveValue, ReserveRequest, ReserveResponse, ReserveStreamRequest, ReserveStreamResponse,
    TraceContext, enqueue_batch_response, nack_retry,
    queue_service_server::{QueueService, QueueServiceServer},
    reserve_stream_request, reserve_stream_response,
};

mod pb;

const DEFAULT_MAX_ATTEMPTS: u32 = 3;
const MAX_LEASE_DURATION_MS: u64 = 5 * 60 * 1000;
const LISTEN_ADDR: &str = "0.0.0.0:50051";

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[derive(Clone)]
struct JobEntry {
    queue: String,
    job_type: String,
    payload: Option<Payload>,
    priority: u32,
    trace_context: Option<TraceContext>,
    enqueued_at: i64,
    attempt: u32,
    max_attempts: u32,
    lease_expires_at: i64,
    custom: HashMap<String, PrimitiveValue>,
    scheduled_at: Option<i64>,
}

impl JobEntry {
    fn to_job(&self, id: String) -> Job {
        Job {
            id,
            job_type: self.job_type.clone(),
            payload: self.payload.clone(),
            priority: self.priority,
            trace_context: self.trace_context.clone(),
            enqueued_at: self.enqueued_at,
            attempt: self.attempt,
            max_attempts: self.max_attempts,
            lease_expires_at: self.lease_expires_at,
            custom: self.custom.clone(),
            scheduled_at: self.scheduled_at,
        }
    }
}

#[derive(Default)]
struct State {
    jobs: HashMap<String, JobEntry>,
    available: HashMap<String, VecDeque<String>>,
    dedup: HashMap<(String, String), String>,
}

impl State {
    fn enqueue(&mut self, req: EnqueueRequest) -> (String, bool) {
        if let Some(idem) = &req.idempotency_key
            && let Some(existing) = self.dedup.get(&(req.queue.clone(), idem.clone()))
        {
            return (existing.clone(), true);
        }

        let id = Uuid::new_v4().to_string();
        let queue = req.queue.clone();
        let entry = JobEntry {
            queue: queue.clone(),
            job_type: req.job_type,
            payload: req.payload,
            priority: req.priority.unwrap_or(0),
            trace_context: req.trace_context,
            enqueued_at: now_ms(),
            attempt: 1,
            max_attempts: req.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS),
            lease_expires_at: 0,
            custom: req.custom,
            scheduled_at: req.scheduled_at,
        };

        println!("{:?}", entry.custom);

        if let Some(idem) = &req.idempotency_key {
            self.dedup.insert((queue.clone(), idem.clone()), id.clone());
        }

        self.available
            .entry(queue)
            .or_default()
            .push_back(id.clone());
        self.jobs.insert(id.clone(), entry);
        (id, false)
    }

    fn try_reserve(&mut self, queues: &[String], lease_duration_ms: u64) -> Option<Job> {
        for q in queues {
            while let Some(id) = self.available.get_mut(q).and_then(|d| d.pop_front()) {
                if let Some(entry) = self.jobs.get_mut(&id) {
                    entry.lease_expires_at = now_ms() + lease_duration_ms as i64;
                    return Some(entry.to_job(id));
                }
            }
        }
        None
    }

    fn ack(&mut self, id: &str, attempt: u32) -> Result<(), Status> {
        match self.jobs.get(id) {
            Some(entry) if entry.attempt == attempt => {
                self.jobs.remove(id);
                Ok(())
            }
            Some(_) => Err(Status::failed_precondition("attempt mismatch")),
            None => Err(Status::not_found("job not found")),
        }
    }

    fn nack(&mut self, req: &NackRequest) -> Result<bool, Status> {
        let entry = self
            .jobs
            .get_mut(&req.job_id)
            .ok_or_else(|| Status::not_found("job not found"))?;
        if entry.attempt != req.attempt {
            return Err(Status::failed_precondition("attempt mismatch"));
        }

        let force_dl = matches!(
            req.retry.as_ref().and_then(|r| r.strategy.as_ref()),
            Some(nack_retry::Strategy::DeadLetter(_))
        );
        if force_dl || entry.attempt >= entry.max_attempts {
            self.jobs.remove(&req.job_id);
            return Ok(true);
        }

        entry.attempt += 1;
        let q = entry.queue.clone();
        self.available
            .entry(q)
            .or_default()
            .push_back(req.job_id.clone());
        Ok(false)
    }

    fn extend(&mut self, req: &ExtendRequest) -> Result<i64, Status> {
        let entry = self
            .jobs
            .get_mut(&req.job_id)
            .ok_or_else(|| Status::not_found("job not found"))?;
        if entry.attempt != req.attempt {
            return Err(Status::failed_precondition("attempt mismatch"));
        }
        entry.lease_expires_at = now_ms() + req.lease_duration_ms as i64;
        Ok(entry.lease_expires_at)
    }
}

#[derive(Default)]
pub struct QueueServer {
    validator: Validator,
    state: Arc<Mutex<State>>,
    notify: Arc<Notify>,
}

struct RxStream<T>(mpsc::Receiver<T>);

impl<T> Stream for RxStream<T> {
    type Item = T;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.0.poll_recv(cx)
    }
}

fn status_to_error(s: Status) -> ErrorDetails {
    ErrorDetails {
        code: format!("{:?}", s.code()),
        message: s.message().to_string(),
        context: HashMap::new(),
    }
}

#[tonic::async_trait]
impl QueueService for QueueServer {
    async fn enqueue_batch(
        &self,
        request: Request<EnqueueBatchRequest>,
    ) -> Result<Response<EnqueueBatchResponse>, Status> {
        let req = request.into_inner();
        self.validator
            .validate(&req)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let job_responses: Vec<EnqueueResponse> = {
            let mut state = self.state.lock().await;
            req.jobs
                .into_iter()
                .map(|j| {
                    let (job_id, deduplicated) = state.enqueue(j);
                    EnqueueResponse {
                        job_id,
                        deduplicated,
                    }
                })
                .collect()
        };
        self.notify.notify_waiters();

        Ok(Response::new(EnqueueBatchResponse {
            result: Some(enqueue_batch_response::Result::Success(BatchSuccess {
                job_responses,
            })),
        }))
    }

    async fn reserve(
        &self,
        request: Request<ReserveRequest>,
    ) -> Result<Response<ReserveResponse>, Status> {
        let req = request.into_inner();
        self.validator
            .validate(&req)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let lease = req.lease_duration_ms.min(MAX_LEASE_DURATION_MS);
        let deadline = Instant::now() + Duration::from_millis(req.wait_timeout_ms);

        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            {
                let mut state = self.state.lock().await;
                if let Some(job) = state.try_reserve(&req.queues, lease) {
                    return Ok(Response::new(ReserveResponse { job: Some(job) }));
                }
            }

            if Instant::now() >= deadline {
                return Ok(Response::new(ReserveResponse { job: None }));
            }

            tokio::select! {
                _ = &mut notified => {}
                _ = sleep_until(deadline) => {
                    return Ok(Response::new(ReserveResponse { job: None }));
                }
            }
        }
    }

    type ReserveStreamStream =
        Pin<Box<dyn Stream<Item = Result<ReserveStreamResponse, Status>> + Send + 'static>>;

    async fn reserve_stream(
        &self,
        request: Request<Streaming<ReserveStreamRequest>>,
    ) -> Result<Response<Self::ReserveStreamStream>, Status> {
        let mut incoming = request.into_inner();
        let state = self.state.clone();
        let notify = self.notify.clone();

        let first = incoming
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("stream closed before init"))?;
        let init = match first.message {
            Some(reserve_stream_request::Message::Init(i)) => i,
            _ => return Err(Status::invalid_argument("first message must be init")),
        };

        let queues = init.queues;
        let lease = init.lease_duration_ms.min(MAX_LEASE_DURATION_MS);
        let mut max_in_flight = init.max_in_flight;

        let (tx, rx) = mpsc::channel::<Result<ReserveStreamResponse, Status>>(32);

        tokio::spawn(async move {
            let mut in_flight: u32 = 0;

            loop {
                let notified = notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();

                if in_flight < max_in_flight {
                    let job_opt = {
                        let mut state_g = state.lock().await;
                        state_g.try_reserve(&queues, lease)
                    };
                    if let Some(job) = job_opt {
                        in_flight += 1;
                        if tx
                            .send(Ok(ReserveStreamResponse {
                                message: Some(reserve_stream_response::Message::Job(job)),
                            }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        continue;
                    }
                }

                tokio::select! {
                    biased;
                    msg = incoming.message() => {
                        let req = match msg {
                            Ok(Some(r)) => r,
                            _ => return,
                        };
                        match req.message {
                            Some(reserve_stream_request::Message::Init(_)) => {
                                let _ = tx.send(Ok(ReserveStreamResponse {
                                    message: Some(reserve_stream_response::Message::Error(ErrorDetails {
                                        code: "ALREADY_INITIALIZED".into(),
                                        message: "init received twice".into(),
                                        context: HashMap::new(),
                                    })),
                                })).await;
                            }
                            Some(reserve_stream_request::Message::ChangeCap(c)) => {
                                max_in_flight = c.max_in_flight;
                            }
                            Some(reserve_stream_request::Message::Ack(a)) => {
                                let result = state.lock().await.ack(&a.job_id, a.attempt);
                                let resp = match result {
                                    Ok(()) => {
                                        in_flight = in_flight.saturating_sub(1);
                                        reserve_stream_response::Message::AckResponse(AckResponse { job_id: a.job_id })
                                    }
                                    Err(s) => reserve_stream_response::Message::Error(status_to_error(s)),
                                };
                                let _ = tx.send(Ok(ReserveStreamResponse { message: Some(resp) })).await;
                            }
                            Some(reserve_stream_request::Message::Nack(n)) => {
                                let result = state.lock().await.nack(&n);
                                let resp = match result {
                                    Ok(dead_lettered) => {
                                        in_flight = in_flight.saturating_sub(1);
                                        if !dead_lettered {
                                            notify.notify_waiters();
                                        }
                                        reserve_stream_response::Message::NackResponse(NackResponse {
                                            job_id: n.job_id,
                                            dead_lettered,
                                        })
                                    }
                                    Err(s) => reserve_stream_response::Message::Error(status_to_error(s)),
                                };
                                let _ = tx.send(Ok(ReserveStreamResponse { message: Some(resp) })).await;
                            }
                            Some(reserve_stream_request::Message::Extend(e)) => {
                                let result = state.lock().await.extend(&e);
                                let resp = match result {
                                    Ok(lease_expires_at) => reserve_stream_response::Message::ExtendResponse(ExtendResponse {
                                        job_id: e.job_id,
                                        lease_expires_at,
                                    }),
                                    Err(s) => reserve_stream_response::Message::Error(status_to_error(s)),
                                };
                                let _ = tx.send(Ok(ReserveStreamResponse { message: Some(resp) })).await;
                            }
                            None => {}
                        }
                    }
                    _ = &mut notified, if in_flight < max_in_flight => {}
                }
            }
        });

        let stream: Self::ReserveStreamStream = Box::pin(RxStream(rx));
        Ok(Response::new(stream))
    }

    async fn ack(&self, request: Request<AckRequest>) -> Result<Response<AckResponse>, Status> {
        let req = request.into_inner();
        self.validator
            .validate(&req)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        self.state.lock().await.ack(&req.job_id, req.attempt)?;
        Ok(Response::new(AckResponse { job_id: req.job_id }))
    }

    async fn nack(&self, request: Request<NackRequest>) -> Result<Response<NackResponse>, Status> {
        let req = request.into_inner();
        self.validator
            .validate(&req)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let dead_lettered = self.state.lock().await.nack(&req)?;
        if !dead_lettered {
            self.notify.notify_waiters();
        }
        Ok(Response::new(NackResponse {
            job_id: req.job_id,
            dead_lettered,
        }))
    }

    async fn extend(
        &self,
        request: Request<ExtendRequest>,
    ) -> Result<Response<ExtendResponse>, Status> {
        let req = request.into_inner();
        self.validator
            .validate(&req)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let lease_expires_at = self.state.lock().await.extend(&req)?;
        Ok(Response::new(ExtendResponse {
            job_id: req.job_id,
            lease_expires_at,
        }))
    }

    async fn get_server_info(
        &self,
        _request: Request<GetServerInfoRequest>,
    ) -> Result<Response<GetServerInfoResponse>, Status> {
        Ok(Response::new(GetServerInfoResponse {
            server_version: env!("CARGO_PKG_VERSION").to_string(),
            supported_protocol_versions: vec!["1.0".to_string()],
            server_time_ms: now_ms(),
            allowed_encodings: vec!["application/json".into(), "application/octet-stream".into()],
            max_payload_bytes: 1 << 20,
            max_custom_entries: 64,
            max_custom_total_bytes: 16 << 10,
            max_custom_key_bytes: 256,
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = LISTEN_ADDR.parse()?;
    let svc = QueueServer::default();
    println!("sepp queue server listening on {addr}");
    Server::builder()
        .add_service(QueueServiceServer::new(svc))
        .serve(addr)
        .await?;
    Ok(())
}
