//! Shared transport setup for provider protocol tests. Reader contracts live in test-support.
use open_arrow_access_service::remote::Connection;
use open_arrow_access_test_support::http::HttpFixture;
use opendal::OperationContext;

pub fn connection(fixture: &HttpFixture) -> Connection {
    Connection::new(fixture.endpoint())
        .unwrap()
        .with_context(OperationContext::new())
}
