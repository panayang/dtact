#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Setup directories
rm -r ./target/github_release
mkdir -p ./target/github_release
export GPG_TTY=$(tty)

# 1. Build main library
echo "Building main library..."
cargo build --release

# 2. Copy main library artifacts
# Note: Adjust the filenames below to match your actual build output names
cp ./target/release/libdtact.a ./target/github_release/
cp ./target/release/libdtact.rlib ./target/github_release/
cp ./target/release/libdtact.so ./target/github_release/

# 3. Copy main headers
cp ./dtact.h ./target/github_release/
cp ./dtact.hpp ./target/github_release/

# 4. Build util library (native)
echo "Building util (native)..."
cd ./dtact-util
cargo build --release --features native,ffi
cp ../target/release/libdtact_util.a ../target/github_release/libdtact_util_native.a
cp ../target/release/libdtact_util.rlib ../target/github_release/libdtact_util_native.rlib
cp ../target/release/libdtact_util.so ../target/github_release/libdtact_util_native.so

# 5. Build util library (tokio)
echo "Building util (tokio)..."
cargo build --release --features tokio,ffi
cp ../target/release/libdtact_util.a ../target/github_release/libdtact_util_tokio.a
cp ../target/release/libdtact_util.rlib ../target/github_release/libdtact_util_tokio.rlib
cp ../target/release/libdtact_util.so ../target/github_release/libdtact_util_tokio.so

# 6. Copy util headers
cp ./dtact_util.h ../target/github_release/
cp ./dtact_util.hpp ../target/github_release/

# 7. GPG Signing
echo "Signing artifacts..."
cd ../target/github_release
for file in *; do
    if [ -f "$file" ]; then
        gpg --detach-sign --armor "$file"
    fi
done

echo "Release artifacts prepared in ./target/github_release"
