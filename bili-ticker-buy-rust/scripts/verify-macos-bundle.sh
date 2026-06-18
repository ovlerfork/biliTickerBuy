#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "verify-macos-bundle: macOS only"
  exit 0
fi

shopt -s nullglob

apps=(src-tauri/target/release/bundle/macos/*.app)
if (( ${#apps[@]} == 0 )); then
  echo "No macOS .app bundle found"
  exit 1
fi

for app in "${apps[@]}"; do
  codesign --verify --deep --strict --verbose=4 "$app"
done

dmgs=(src-tauri/target/release/bundle/dmg/*.dmg)
for dmg in "${dmgs[@]}"; do
  mount_dir="$(mktemp -d)"
  hdiutil attach "$dmg" -mountpoint "$mount_dir" -nobrowse -quiet
  trap 'hdiutil detach "$mount_dir" -quiet || true; rmdir "$mount_dir" || true' EXIT

  mounted_apps=("$mount_dir"/*.app)
  if (( ${#mounted_apps[@]} == 0 )); then
    echo "No .app bundle found in $dmg"
    exit 1
  fi

  for app in "${mounted_apps[@]}"; do
    codesign --verify --deep --strict --verbose=4 "$app"
  done

  hdiutil detach "$mount_dir" -quiet
  rmdir "$mount_dir"
  trap - EXIT
done
