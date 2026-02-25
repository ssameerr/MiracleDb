#!/bin/bash
# MiracleDb Cross-Platform Release Build Script
set -e

VERSION=${1:-"0.1.0"}
OUTPUT_DIR="release"

echo "🚀 Building MiracleDb v${VERSION} for all platforms..."

mkdir -p "$OUTPUT_DIR"

# Linux x86_64 (static musl)
echo "📦 Building Linux x86_64 (musl)..."
if command -v cross &> /dev/null; then
    cross build --release --target x86_64-unknown-linux-musl
    cp target/x86_64-unknown-linux-musl/release/miracledb "$OUTPUT_DIR/miracledb-${VERSION}-linux-x86_64"
else
    echo "⚠️  'cross' not found, building native Linux..."
    cargo build --release
    cp target/release/miracledb "$OUTPUT_DIR/miracledb-${VERSION}-linux-x86_64"
fi

# Linux ARM64 (if cross available)
if command -v cross &> /dev/null; then
    echo "📦 Building Linux ARM64 (musl)..."
    cross build --release --target aarch64-unknown-linux-musl
    cp target/aarch64-unknown-linux-musl/release/miracledb "$OUTPUT_DIR/miracledb-${VERSION}-linux-arm64"
fi

# Windows x86_64 (if cross available)
if command -v cross &> /dev/null; then
    echo "📦 Building Windows x86_64..."
    cross build --release --target x86_64-pc-windows-gnu
    cp target/x86_64-pc-windows-gnu/release/miracledb.exe "$OUTPUT_DIR/miracledb-${VERSION}-windows-x86_64.exe"
fi

# macOS builds require macOS host or osxcross
echo "ℹ️  macOS builds require macOS host. Run on macOS:"
echo "    cargo build --release --target x86_64-apple-darwin"
echo "    cargo build --release --target aarch64-apple-darwin"

echo ""
echo "✅ Build complete! Binaries in $OUTPUT_DIR/"
ls -lh "$OUTPUT_DIR/"
