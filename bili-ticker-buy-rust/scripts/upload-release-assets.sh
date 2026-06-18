#!/usr/bin/env bash
set -euo pipefail

version="${1:?version is required}"
tag="v$version"

shopt -s nullglob
files=(
  src-tauri/target/release/bundle/dmg/*.dmg
  src-tauri/target/release/bundle/msi/*.msi
  src-tauri/target/release/bundle/nsis/*.exe
)

if (( ${#files[@]} == 0 )); then
  echo "No release assets found"
  exit 1
fi

if gh release view "$tag" >/dev/null 2>&1; then
  gh release edit "$tag" --draft=false --latest --title "$tag" --notes "Automated release $tag"
else
  gh release create "$tag" --title "$tag" --notes "Automated release $tag" --latest || gh release view "$tag" >/dev/null
fi

gh release upload "$tag" "${files[@]}" --clobber
