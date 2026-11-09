//! Read protocol metadata with the same XML engine as the XML file format.
mod error;
mod parser;
mod validation;

pub use parser::{XmlDocument, validate_root};
