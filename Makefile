# filter out target and keep the rest as args
PRIMARY_TARGET := $(firstword $(MAKECMDGOALS))
ARGS := $(filter-out $(PRIMARY_TARGET), $(MAKECMDGOALS))

.PHONY: git-hooks
git-hooks:
	git config core.hooksPath ./git-hooks;

.PHONY: init
init: git-hooks

.PHONY: fmt
fmt: init
	cargo fmt

.PHONY: test-all
test-all: test-codec test-stream test-api test
	echo run all tests

.PHONY: test-codec
	cargo check -p razor-rpc-codec
	cargo test -p razor-rpc-codec

.PHONY: test-stream
test-stream: init
	cargo test -p razor-stream -- --nocapture

.PHONY: test-api
test-api: init
	RUST_BACKTRACE=1 cargo test -p razor-rpc -- --nocapture

# usage:
# make test-stream "test_normal --F tokio"
# make test-stream "test_normal --F smol"
# make test-stream - "--features smol"
.PHONY: test
test: init
	@echo "Run integration tests"
	cargo test -p razor-rpc-test ${ARGS} -- --nocapture --test-threads=1
	@echo "Done"

pressure: init
	cargo test -p razor-rpc-test ${ARGS} --release -- --nocapture --test-threads=1

.PHONY: build
build: init
	cargo build -p razor-stream-macros
	cargo build -p razor-stream
	cargo build -p razor-rpc-tcp
	cargo build -p razor-rpc-test
	cargo build

.DEFAULT_GOAL = build

# Target name % means that it is a rule that matches anything, @: is a recipe;
# the : means do nothing
%:
	@:
