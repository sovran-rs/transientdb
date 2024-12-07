#!/bin/bash

# if anything fails, we don't release.
set -euo pipefail

# Run cargo fmt
echo "Running cargo fmt..."
cargo fmt

# Run clippy
echo "Running clippy..."
cargo clippy --all-features -- -D warnings

# Run all tests
echo "Running tests: '$> cargo test'"
cargo test &> /dev/null

# Run all examples
echo "Running examples..."
for example in examples/*.rs; do
    example_name=$(basename "$example" .rs)
    echo "Running example: '$> cargo run --example $example_name'"
    cargo run --example "$example_name" &> /dev/null
done

# Check if gh is installed
if ! command -v gh &> /dev/null; then
    echo "Error: GitHub CLI (gh) is not installed. Please install it first:"
    echo "  brew install gh"
    exit 1
fi

# Check if we're on main or master branch
CURRENT_BRANCH=$(git branch --show-current)
if [ "$CURRENT_BRANCH" != "main" ] && [ "$CURRENT_BRANCH" != "master" ]; then
    echo "Error: Must be on 'main' or 'master' branch to release. Currently on '$CURRENT_BRANCH'"
    exit 1
fi

# Run release script
echo "Running release process..."
cargo run --bin release