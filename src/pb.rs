use std::sync::LazyLock;

use prost_reflect::DescriptorPool;

pub static DESCRIPTOR_POOL: LazyLock<DescriptorPool> = LazyLock::new(|| {
    DescriptorPool::decode(
        include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin")).as_ref(),
    )
    .expect("failed to decode sepp file descriptor set")
});

pub mod sepp {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/sepp.v1.rs"));
    }
}

// Internally we use ms since epoch so we need to convert at the wire boundary.

pub fn millis_to_timestamp(ms: i64) -> ::prost_types::Timestamp {
    let mut ts = ::prost_types::Timestamp {
        seconds: ms.div_euclid(1000),
        nanos: (ms.rem_euclid(1000) * 1_000_000) as i32,
    };
    ts.normalize();
    ts
}

pub fn timestamp_to_millis(ts: &::prost_types::Timestamp) -> i64 {
    let norm = ts.normalized();
    norm.seconds
        .saturating_mul(1000)
        .saturating_add((norm.nanos / 1_000_000) as i64)
}

pub fn millis_to_duration(ms: u64) -> ::prost_types::Duration {
    ::prost_types::Duration {
        seconds: (ms / 1000) as i64,
        nanos: ((ms % 1000) * 1_000_000) as i32,
    }
}

pub fn duration_to_millis(d: &::prost_types::Duration) -> u64 {
    let norm = d.normalized();
    let secs = norm.seconds.max(0) as u64;
    let nanos_ms = (norm.nanos.max(0) / 1_000_000) as u64;
    secs.saturating_mul(1000).saturating_add(nanos_ms)
}
