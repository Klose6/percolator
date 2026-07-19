//! Percolator: a small MVCC transaction stack (TSO + client 2PC + in-memory store).
//!
//! Module layout:
//! - `client` — transaction client (`begin` / `get` / `set` / `commit`)
//! - `server` — `TimestampOracle` and `MemoryStorage` (MVCC Bigtable columns)
//! - `service` — RPC service definitions expanded by `service!`
//! - `msg` — protobuf message types generated from `proto/msg.proto`

/// This crate re-exports itself as `labrpc` so `service.rs` can use `labrpc::service!`
/// without depending on a full network labrpc implementation.
extern crate self as labrpc;

pub type Result<T> = std::io::Result<T>;

/// Declares an RPC service module with a server [`Service`] trait and a sync [`Client`] stub.
///
/// Example input ([`service`](crate::service)):
/// ```ignore
/// service timestamp {
///     rpc get_timestamp(TimestampRequest) returns (TimestampResponse);
/// }
/// ```
///
/// Expands to module `timestamp` containing:
/// - `trait Service` — async handlers implemented by the server (e.g. `TimestampOracle`)
/// - `struct Client` — sync stub; bind with `Client::with_service(...)`, then call RPCs
///
/// Each generated client method `block_on`s the matching `Service` method.
#[macro_export]
macro_rules! service {
    (service $service_name:ident { $(rpc $method:ident($req:ty) returns ($res:ty);)* }) => {
        pub mod $service_name {
            use std::sync::Arc;

            use crate::msg::*;
            use crate::Result;

            /// Server-side RPC interface for this service.
            #[async_trait::async_trait]
            pub trait Service: Send + Sync {
                $(async fn $method(&self, _req: $req) -> Result<$res>;)*
            }

            /// Sync RPC client stub. Bind a [`Service`] so calls reach the server.
            #[derive(Clone, Default)]
            pub struct Client {
                service: Option<Arc<dyn Service>>,
            }

            impl Client {
                /// Unbound client; RPC calls fail until [`with_service`](Self::with_service).
                pub fn new() -> Self {
                    Self { service: None }
                }

                /// Attach the server-side implementation this stub should invoke.
                pub fn with_service<S: Service + 'static>(service: S) -> Self {
                    Self {
                        service: Some(Arc::new(service)),
                    }
                }

                // One sync method per `rpc` line in the service definition.
                $(
                pub fn $method(&self, req: $req) -> Result<$res> {
                    let service = self.service.as_ref().ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::NotConnected,
                            concat!(stringify!($method), ": no service bound"),
                        )
                    })?;
                    // Service handlers are async; run them to completion on this thread.
                    futures::executor::block_on(service.$method(req))
                }
                )*
            }

            /// Placeholder for registering a service with a real RPC network (unused stub).
            pub fn add_service<S: Service + 'static>(_service: S) {}
        }
    };
}

#[allow(unused_imports)]
#[macro_use]
extern crate log;

// After you finish the implementation, `#[allow(unused)]` should be removed.
#[allow(dead_code, unused)]
mod client;
#[allow(unused)]
mod server;
mod service;
#[cfg(test)]
mod tests;

/// Protobuf message types generated at build time from `proto/msg.proto`.
mod msg {
    include!(concat!(env!("OUT_DIR"), "/_.rs"));
}
