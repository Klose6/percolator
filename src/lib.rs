extern crate self as labrpc;

pub type Result<T> = std::io::Result<T>;

#[macro_export]
macro_rules! service {
    (service $service_name:ident { $(rpc $method:ident($req:ty) returns ($res:ty);)* }) => {
        pub mod $service_name {
            use std::sync::Arc;

            use crate::msg::*;
            use crate::Result;

            #[async_trait::async_trait]
            pub trait Service: Send + Sync {
                $(async fn $method(&self, _req: $req) -> Result<$res>;)*
            }

            /// RPC client stub. Bind a [`Service`] implementation so calls reach the server.
            #[derive(Clone, Default)]
            pub struct Client {
                service: Option<Arc<dyn Service>>,
            }

            impl Client {
                pub fn new() -> Self {
                    Self { service: None }
                }

                /// Attach the server-side service this client should call.
                pub fn with_service<S: Service + 'static>(service: S) -> Self {
                    Self {
                        service: Some(Arc::new(service)),
                    }
                }

                $(
                pub fn $method(&self, req: $req) -> Result<$res> {
                    let service = self.service.as_ref().ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::NotConnected,
                            concat!(stringify!($method), ": no service bound"),
                        )
                    })?;
                    futures::executor::block_on(service.$method(req))
                }
                )*
            }

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

// This is related to protobuf as described in `msg.proto`.
mod msg {
    include!(concat!(env!("OUT_DIR"), "/_.rs"));
}
