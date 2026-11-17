//! Shared transport setup for provider protocol tests. Reader contracts live in test-support.
use open_arrow_access_service::common::Connection;
use open_arrow_access_test_support::http::HttpFixture;

pub fn connection(fixture: &HttpFixture) -> Connection {
    connection_at(fixture.endpoint(), fixture.context())
}

/// Construct a provider connection for owned-endpoint resolution cases.
pub fn connection_at(endpoint: &str, context: opendal::OperationContext) -> Connection {
    Connection::new(endpoint).unwrap().with_context(context)
}
