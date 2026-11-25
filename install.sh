#!/usr/bin/env bash
set -euo pipefail

main() {
  local target bin dir
  target="$(uname -s)-$(uname -m)"
  case "$target" in
    Darwin-x86_64|Darwin-arm64|Linux-x86_64|Linux-aarch64) ;;
    *) echo "No tinyq release for $target." >&2; return 1 ;;
  esac

  bin="$HOME/.local/bin"
  for dir in "$bin" "$HOME/bin" /usr/local/bin /opt/homebrew/bin; do
    if [[ ":$PATH:" == *":$dir:"* && -d "$dir" && -w "$dir" ]]; then
      bin="$dir"
      break
    fi
  done
  mkdir -p "$bin"
  tmp=$(mktemp -d "$bin/.tinyq.XXXXXXXX")
  trap 'rm -rf -- "$tmp"' EXIT
  curl -fsSL "https://github.com/Chadi00/tinyq/releases/latest/download/tinyq-$target.tar.gz" |
    tar -xOzf - tinyq > "$tmp/tinyq"
  chmod 755 "$tmp/tinyq"
  "$tmp/tinyq" --help > /dev/null
  mv -f "$tmp/tinyq" "$bin/tinyq"
  echo "Installed tinyq in $bin."
  if [[ ":$PATH:" != *":$bin:"* ]]; then
    echo 'Add this to your shell profile and run it in this terminal:'
    echo '  export PATH="$HOME/.local/bin:$PATH"'
  fi
}

main
