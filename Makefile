.PHONY: lint lint-rust lint-toml lint-deslop install-deslop fmt fmt-check

DESLOP_VERSION := 0.1.3

lint: fmt-check lint-toml lint-rust lint-deslop

fmt:
	cargo fmt --all
	taplo fmt

fmt-check:
	cargo fmt --all -- --check
	taplo fmt --check

lint-toml:
	taplo lint --no-schema
	cargo metadata --all-features --locked --no-deps --format-version 1 > /dev/null

lint-rust:
	cargo clippy --workspace --all-targets --all-features --locked -- -D warnings

# Use the analyzer shipped with the toolchain pinned in rust-toolchain.toml.
install-deslop:
	cargo install deslop-please --version $(DESLOP_VERSION) --locked

lint-deslop:
	PATH="$$(dirname "$$(rustup which rust-analyzer)"):$${CARGO_HOME:-$$HOME/.cargo}/bin:$$PATH" deslop-please --allow .deslop-please.allow
