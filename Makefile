.PHONY: dev
dev:
	cargo watch -c -x 'clippy' -x 'test' -x 'doc --no-deps'
