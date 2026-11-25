.PHONY: lint lint-rust lint-toml fmt fmt-check

lint: fmt-check lint-toml lint-rust

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
