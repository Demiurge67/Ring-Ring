.PHONY: build test clean flutter-run

build:
	cargo build --release

test:
	cargo test --all

clean:
	cargo clean
	cd ring-ring-flutter && flutter clean

flutter-run:
	cd ring-ring-flutter && flutter run

doc:
	cargo doc --no-deps --open
