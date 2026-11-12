#!/usr/bin/env bash
# Releases the version in mix.exs: tags it, waits for
# .github/workflows/release.yml to build the NIFs, publishes the GitHub
# release, writes the checksum file and publishes the package to Hex.
#
#   scripts/release.sh             release
#   scripts/release.sh --dry-run   print the commands a release would run
#
# A dry run still makes the local checks, and reports the ones that would stop
# a release without stopping itself. It runs nothing that touches the network,
# the repository or Hex.
#
# SKIP_PUBLISH=1 stops before `mix hex.publish`.
set -euo pipefail

REPO="rkallos/peep_rs"
MODULE="Peep.Storage.RustNIF"
CRATE_TOML="native/peep_storage_rustler/Cargo.toml"

DRY_RUN=""
case "${1:-}" in
  "") ;;
  --dry-run) DRY_RUN=1 ;;
  *)
    echo "usage: scripts/release.sh [--dry-run]"
    exit 1
    ;;
esac

# Runs a command, or only prints it on a dry run.
run() {
  if [ -n "$DRY_RUN" ]; then
    echo "    \$ $*"
  else
    "$@"
  fi
}

fail() {
  if [ -n "$DRY_RUN" ]; then
    echo "    would stop here: $1"
  else
    echo "error: $1"
    exit 1
  fi
}

cd "$(dirname "$0")/.."

VERSION=$(sed -n 's/^  @version "\(.*\)"/\1/p' mix.exs | head -n1)
TAG="v${VERSION}"

if [ -z "$VERSION" ]; then
  echo "error: could not extract version from mix.exs"
  exit 1
fi

echo "==> Releasing peep_rs ${TAG}${DRY_RUN:+ (dry run)}"

echo "==> Checking the working tree"

# The precompiled NIF's file name carries the mix.exs version.
CRATE_VERSION=$(sed -n 's/^version = "\(.*\)"/\1/p' "$CRATE_TOML" | head -n1)
if [ "$CRATE_VERSION" != "$VERSION" ]; then
  fail "mix.exs is ${VERSION} but ${CRATE_TOML} is ${CRATE_VERSION}"
fi

if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  fail "uncommitted changes, commit or stash first"
fi

if git rev-parse "$TAG" >/dev/null 2>&1; then
  fail "tag ${TAG} already exists"
fi

if ! command -v gh >/dev/null 2>&1; then
  fail "gh CLI not found"
fi

if [ -z "${SKIP_PUBLISH:-}" ] && [ -z "${HEX_API_KEY:-}" ]; then
  fail "HEX_API_KEY not set (mix hex.user key generate --key-name peep_rs-release --permission api:write), or set SKIP_PUBLISH=1"
fi

# The tag must point at a pushed commit that passed CI: a tag on an unpushed or
# red commit still triggers the release build, and the tag is hard to retract.
echo "==> Checking that HEAD is origin/main and passed CI"
HEAD_SHA=$(git rev-parse HEAD)
run git fetch -q origin main
if [ -n "$DRY_RUN" ]; then
  echo "    (stops unless HEAD ${HEAD_SHA} is origin/main)"
  echo "    \$ gh run list --repo ${REPO} --workflow=ci.yml --commit ${HEAD_SHA} --event push --json databaseId --jq '.[0].databaseId'"
  echo "    \$ gh run watch --repo ${REPO} <ci run id> --exit-status"
else
  if [ "$HEAD_SHA" != "$(git rev-parse origin/main)" ]; then
    fail "HEAD is not origin/main, push main or check it out before releasing"
  fi

  CI_RUN=$(gh run list --repo "$REPO" --workflow=ci.yml --commit "$HEAD_SHA" --event push --json databaseId --jq '.[0].databaseId')
  if [ -z "$CI_RUN" ]; then
    fail "no CI run found for ${HEAD_SHA}"
  fi
  echo "    waiting for CI run ${CI_RUN}"
  if ! gh run watch --repo "$REPO" "$CI_RUN" --exit-status >/dev/null; then
    fail "CI failed for ${HEAD_SHA}: https://github.com/${REPO}/actions/runs/${CI_RUN}"
  fi
fi

echo "==> Tagging ${TAG}"
run git tag "$TAG"
run git push origin "$TAG"

echo "==> Waiting for the release workflow to build the NIFs"
if [ -n "$DRY_RUN" ]; then
  echo "    \$ gh run list --repo ${REPO} --workflow=release.yml --branch=${TAG} --json databaseId --jq '.[0].databaseId'"
  echo "    \$ gh run watch --repo ${REPO} <release run id> --exit-status"
else
  RUN_ID=""
  for i in $(seq 1 12); do
    sleep 5
    RUN_ID=$(gh run list --repo "$REPO" --workflow=release.yml --branch="$TAG" --json databaseId --jq '.[0].databaseId' 2>/dev/null || true)
    if [ -n "$RUN_ID" ]; then
      break
    fi
    echo "    waiting for the workflow to appear... (${i}/12)"
  done

  if [ -z "$RUN_ID" ]; then
    fail "could not find the release workflow run, check https://github.com/${REPO}/actions"
  fi

  echo "    https://github.com/${REPO}/actions/runs/${RUN_ID}"
  gh run watch --repo "$REPO" "$RUN_ID" --exit-status
fi

echo "==> Publishing the GitHub release"
run gh release edit "$TAG" --repo "$REPO" --draft=false

# RustlerPrecompiled checks every download against this file, which ships in
# the Hex package. The build is forced because compiling the module would
# otherwise need the checksums this writes.
echo "==> Writing checksum-Elixir.${MODULE}.exs"
run env PEEP_RS_BUILD=1 mix rustler_precompiled.download "$MODULE" --all --print

if [ -n "${SKIP_PUBLISH:-}" ]; then
  echo "==> SKIP_PUBLISH is set, so stopping before: mix hex.publish"
  exit 0
fi

echo "==> Publishing to Hex"
run mix hex.publish --yes

echo "==> peep_rs ${TAG} ${DRY_RUN:+would be }published"
