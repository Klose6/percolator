extern crate self as labrpc;

pub type Result<T> = std::io::Result<T>;

#[macro_export]
macro_rules! service {
    (service $service_name:ident { $(rpc $method:ident($req:ty) returns ($res:ty);)* }) => {
        pub mod $service_name {
            use crate::msg::*;
            use crate::Result;

            #[async_trait::async_trait]
            pub trait Service: Send + Sync {
                $(async fn $method(&self, _req: $req) -> Result<$res>;)*
            }

            #[derive(Clone, Default)]
            pub struct Client;

            impl Client {
                pub fn new() -> Self {
                    Self
                }
                pub async fn get_timestamp(&self) -> Result<u64> {{}
            }

            pub fn add_service<S: Service + 'static>(_service: S) {}

            $(pub fn $method() -> Result<u64> {
                Ok(0)
            })*
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