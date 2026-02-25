#!/bin/bash
set -e

rustup target add wasm32-unknown-unknown

cargo build --target wasm32-unknown-unknown --release

mkdir -p ../test_data
cp target/wasm32-unknown-unknown/release/wasm_test_modules.wasm ../test_data/add_two_i64.wasm
cp target/wasm32-unknown-unknown/release/wasm_test_modules.wasm ../test_data/sum_three_f64.wasm
cp target/wasm32-unknown-unknown/release/wasm_test_modules.wasm ../test_data/strings.wasm

echo "WASM test modules built successfully!"
echo "Output files:"
echo "  - ../test_data/add_two_i64.wasm"
echo "  - ../test_data/sum_three_f64.wasm"
echo "  - ../test_data/strings.wasm"
