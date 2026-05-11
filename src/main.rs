use std::{collections::VecDeque, sync::Arc, time::SystemTime};

use prost_protovalidate::Validator;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use futures_core::Stream;
use std::pin::Pin;
use uuid::Uuid;

use crate::pb::sepp::v1::{
    AckRequest, AckResponse, BatchSuccess, EnqueueBatchRequest, EnqueueBatchResponse,
    EnqueueRequest, EnqueueResponse, ExtendRequest, ExtendResponse, GetServerInfoRequest,
    GetServerInfoResponse, NackRequest, NackResponse, ReserveRequest, ReserveResponse,
    ReserveStreamRequest, ReserveStreamResponse, enqueue_batch_response,
    queue_service_server::QueueService,
};

mod pb;

#[derive(Default)]
pub struct JobRequestQueue {
    pub requests: Mutex<VecDeque<EnqueueRequest>>,
}

impl JobRequestQueue {
    pub async fn enqueue(&self, request: EnqueueRequest) {
        let mut requests = self.requests.lock().await;
        requests.push_back(request);
    }

    pub async fn reserve(&self) -> Option<EnqueueRequest> {
        let mut requests = self.requests.lock().await;
        requests.pop_front()
    }
}

#[derive(Default)]
pub struct QueueServiceServer {
    validator: Validator,
    requests: Arc<JobRequestQueue>,
}

#[tonic::async_trait]
impl QueueService for QueueServiceServer {
    async fn enqueue_batch(
        &self,
        request: Request<EnqueueBatchRequest>,
    ) -> Result<Response<EnqueueBatchResponse>, Status> {
        let req = request.into_inner();
        let mut job_responses = Vec::new();

        self.validator
            .validate(&req)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        for job in req.jobs {
            println!("Received job: {:?}", job);
            self.requests.enqueue(job).await;

            job_responses.push(EnqueueResponse {
                job_id: Uuid::new_v4().to_string(),
                deduplicated: false,
            });
        }

        Ok(Response::new(EnqueueBatchResponse {
            result: Some(enqueue_batch_response::Result::Success(BatchSuccess {
                job_responses,
            })),
        }))
    }

    async fn reserve(
        &self,
        _request: Request<ReserveRequest>,
    ) -> Result<Response<ReserveResponse>, Status> {
        Err(Status::unimplemented("reserve not yet implemented"))
    }

    type ReserveStreamStream =
        Pin<Box<dyn Stream<Item = Result<ReserveStreamResponse, Status>> + Send + 'static>>;

    async fn reserve_stream(
        &self,
        _request: Request<tonic::Streaming<ReserveStreamRequest>>,
    ) -> Result<Response<Self::ReserveStreamStream>, Status> {
        Err(Status::unimplemented("reserve_stream not yet implemented"))
    }

    async fn ack(&self, _request: Request<AckRequest>) -> Result<Response<AckResponse>, Status> {
        Err(Status::unimplemented("ack not yet implemented"))
    }

    async fn nack(&self, _request: Request<NackRequest>) -> Result<Response<NackResponse>, Status> {
        Err(Status::unimplemented("nack not yet implemented"))
    }

    async fn extend(
        &self,
        _request: Request<ExtendRequest>,
    ) -> Result<Response<ExtendResponse>, Status> {
        Err(Status::unimplemented("extend not yet implemented"))
    }

    async fn get_server_info(
        &self,
        _request: Request<GetServerInfoRequest>,
    ) -> Result<Response<GetServerInfoResponse>, Status> {
        Ok(Response::new(GetServerInfoResponse {
            server_version: env!("CARGO_PKG_VERSION").to_string(),
            supported_protocol_versions: vec!["1.0".to_string()],
            server_time_ms: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello, world!");
    Ok(())
}
