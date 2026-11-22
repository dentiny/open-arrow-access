//! Complete OCR item/part directories with shared, joined metadata.
mod accessor;
mod request;
mod validation;

pub use accessor::BhlShardAccessor;
pub use request::BhlShardReadRequest;

#[cfg(test)]
mod tests;
