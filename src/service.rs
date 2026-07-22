//! RPC service definitions for Percolator.
//!
//! Each `labrpc::service!` block expands into a module with:
//! - `Service` — async trait implemented by the server
//! - `Client` — sync stub; bind with `Client::with_service(...)` then call RPCs

/// Timestamp Oracle (TSO) service: allocates strictly increasing timestamps.
labrpc::service! {
    service timestamp {
        rpc get_timestamp(TimestampRequest) returns (TimestampResponse);
    }
}

/// Client stub for the TSO (`timestamp::Client`).
pub use timestamp::Client as TSOClient;

/// Transaction storage service: snapshot reads, optimistic 2PC, and pessimistic locks.
labrpc::service! {
    service transaction {
        rpc get(GetRequest) returns (GetResponse);
        rpc prewrite(PrewriteRequest) returns (PrewriteResponse);
        rpc commit(CommitRequest) returns (CommitResponse);
        rpc rollback(RollbackRequest) returns (RollbackResponse);
        rpc pessimistic_lock(PessimisticLockRequest) returns (PessimisticLockResponse);
    }
}

/// Client stub for MVCC storage (`transaction::Client`).
pub use transaction::Client as TransactionClient;
