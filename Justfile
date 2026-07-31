set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

linux_target := "x86_64-unknown-linux-musl"
macos_target := "aarch64-apple-darwin"
windows_target := "x86_64-pc-windows-gnu"

tabler_version := env_var_or_default("TABLER_VERSION", "1.4.0")
tabler_icons_version := env_var_or_default("TABLER_ICONS_VERSION", "3.31.0")

default:
    @just --list

fmt:
    cargo fmt

check:
    cargo check --all-targets

test:
    cargo test

assets-update:
    mkdir -p static/vendor/tabler/css static/vendor/tabler/js \
        static/vendor/tabler-icons/css/fonts
    curl -fsSL "https://cdn.jsdelivr.net/npm/@tabler/core@{{ tabler_version }}/dist/css/tabler.min.css" -o static/vendor/tabler/css/tabler.min.css
    curl -fsSL "https://cdn.jsdelivr.net/npm/@tabler/core@{{ tabler_version }}/dist/js/tabler.min.js" -o static/vendor/tabler/js/tabler.min.js
    curl -fsSL "https://cdn.jsdelivr.net/npm/@tabler/icons-webfont@{{ tabler_icons_version }}/dist/tabler-icons.min.css" -o static/vendor/tabler-icons/css/tabler-icons.min.css
    curl -fsSL "https://cdn.jsdelivr.net/npm/@tabler/icons-webfont@{{ tabler_icons_version }}/dist/fonts/tabler-icons.woff2" -o static/vendor/tabler-icons/css/fonts/tabler-icons.woff2
    curl -fsSL "https://cdn.jsdelivr.net/npm/@tabler/icons-webfont@{{ tabler_icons_version }}/dist/fonts/tabler-icons.woff" -o static/vendor/tabler-icons/css/fonts/tabler-icons.woff
    just assets-check

assets-check:
    test -s static/favicon.svg
    test -s static/vendor/tabler/css/tabler.min.css
    test -s static/vendor/tabler/js/tabler.min.js
    test -s static/vendor/tabler-icons/css/tabler-icons.min.css
    test -s static/vendor/tabler-icons/css/fonts/tabler-icons.woff2
    test -s static/vendor/tabler-icons/css/fonts/tabler-icons.woff

build-linux: assets-check
    cargo zigbuild --release --target {{ linux_target }}

build-macos: assets-check
    cargo build --release --target {{ macos_target }}

build-windows: assets-check
    cargo build --release --target {{ windows_target }}

build-all: build-linux build-macos build-windows
