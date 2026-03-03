pub mod encoding;
pub mod engine;
pub mod index;
pub mod model;
pub mod query;
pub mod server;
pub mod storage;

fn main() {
    println!("PulseDB v{}", env!("CARGO_PKG_VERSION"));
}
