#!/bin/bash
set -e

# Configuration
VERSION=${1:-"0.1.0"}
PROJECT_NAME="miracledb"
TARGET_DIR="target"
RELEASE_DIR="release"
DIST_DIR="dist"

# Detect Host OS
HOST_OS=$(uname -s | tr '[:upper:]' '[:lower:]')
HOST_ARCH=$(uname -m)

if [ "$HOST_ARCH" == "x86_64" ]; then
    HOST_ARCH="x86_64"
elif [ "$HOST_ARCH" == "aarch64" ]; then
    HOST_ARCH="arm64"
fi

echo "🚀 Packaging $PROJECT_NAME v$VERSION on $HOST_OS-$HOST_ARCH..."

mkdir -p "$DIST_DIR"
mkdir -p "$RELEASE_DIR"

# Helper function to package for a target
package_target() {
    local target_triple=$1
    local os_name=$2
    local arch_name=$3
    local binary_ext=$4
    
    local package_name="${PROJECT_NAME}-v${VERSION}-${os_name}-${arch_name}"
    local staging_dir="${RELEASE_DIR}/${package_name}"
    
    echo "📦 Packaging for $os_name-$arch_name..."
    
    # Check if binary exists
    local bin_path="${TARGET_DIR}/${target_triple}/release/${PROJECT_NAME}${binary_ext}"
    # Fallback for native build (no target triple in path usually if --target not passed, but cargo build --release puts it in target/release)
    if [ "$target_triple" == "native" ]; then
        bin_path="${TARGET_DIR}/release/${PROJECT_NAME}${binary_ext}"
    fi

    if [ ! -f "$bin_path" ]; then
        echo "❌ Binary not found at $bin_path. Skipping packaging for this target."
        return
    fi

    # Create staging directory
    rm -rf "$staging_dir"
    mkdir -p "$staging_dir"

    # Copy files
    cp "$bin_path" "$staging_dir/"
    
    # Try to copy standard text files if they exist in repo root or crate root
    for file in README.md LICENSE INSTALL.md miracledb.toml; do
        if [ -f "../../$file" ]; then
             cp "../../$file" "$staging_dir/"
        elif [ -f "$file" ]; then
             cp "$file" "$staging_dir/"
        fi
    done

    # Create archive
    local archive_name="${package_name}"
    if [ "$os_name" == "windows" ]; then
        archive_name="${archive_name}.zip"
        # zip not always available, use powershell if on windows or zip if on linux
        # zip not always available, use powershell if on windows or zip if on linux
        if command -v zip &> /dev/null; then
            (cd "$RELEASE_DIR" && zip -r "../$DIST_DIR/$archive_name" "$package_name")
        elif command -v python3 &> /dev/null; then
            echo "⚠️  'zip' not found. Using Python 3 to create zip archive..."
            python3 -c "import shutil; shutil.make_archive('$DIST_DIR/${archive_name%.zip}', 'zip', '$RELEASE_DIR', '$package_name')"
        else
            echo "⚠️  'zip' command not found. Skipping archive creation."
        fi
    else
        archive_name="${archive_name}.tar.gz"
        tar -C "$RELEASE_DIR" -czf "$DIST_DIR/$archive_name" "$package_name"
    fi

    echo "✅ Created $DIST_DIR/$archive_name"
}

# 1. Build Native (Linux usually)
echo "🛠️  Building native release..."
cargo build --release

# Package Native
if [ "$HOST_OS" == "linux" ]; then
    package_target "native" "linux" "$HOST_ARCH" ""
elif [ "$HOST_OS" == "darwin" ]; then
    package_target "native" "macos" "$HOST_ARCH" ""
fi

# 2. Cross-compilation (if available) - This section tries to use 'cross' if installed
if command -v cross &> /dev/null; then
    echo "🔄 'cross' detected. Attempting cross-compilation..."
    
    # Linux ARM64
    echo "🛠️  Building Linux ARM64..."
    cross build --release --target aarch64-unknown-linux-musl
    package_target "aarch64-unknown-linux-musl" "linux" "arm64" ""
    

else
    echo "ℹ️  'cross' tool not found. Skipping Linux ARM64 build."
fi

# 3. Windows x64 (via MinGW)
if command -v x86_64-w64-mingw32-gcc &> /dev/null; then
    echo "🛠️  Building Windows x64..."
    cargo build --release --target x86_64-pc-windows-gnu
    package_target "x86_64-pc-windows-gnu" "windows" "x86_64" ".exe"
else
    echo "⚠️  MinGW linker (x86_64-w64-mingw32-gcc) not found. Skipping Windows cross-compilation."
    echo "    To build for Windows, install: sudo apt-get install mingw-w64"
fi

echo ""
echo "✨ Packaging Complete! Artifacts in $DIST_DIR/"
ls -lh "$DIST_DIR/"
