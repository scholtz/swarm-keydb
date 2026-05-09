#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <chart-version>"
  exit 1
fi

VERSION="$1"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART_DIR="${REPO_ROOT}/helm/swarm-keydb"
PAGES_URL="https://scholtz.github.io/swarm-keydb/"
PACKAGE_DIR="${REPO_ROOT}/.cr-release-packages"
WORKTREE_DIR="/tmp/swarm-keydb-gh-pages"

for cmd in git helm sed; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Required command not found: $cmd"
    exit 1
  fi
done

sed -i.bak -E "s/^version: .*/version: ${VERSION}/" "${CHART_DIR}/Chart.yaml"
sed -i.bak -E "s/^appVersion: .*/appVersion: \"${VERSION}\"/" "${CHART_DIR}/Chart.yaml"
rm -f "${CHART_DIR}/Chart.yaml.bak"

helm lint "${CHART_DIR}"
mkdir -p "${PACKAGE_DIR}"
helm package "${CHART_DIR}" --destination "${PACKAGE_DIR}"

rm -rf "${WORKTREE_DIR}"

git -C "${REPO_ROOT}" fetch origin gh-pages:refs/heads/gh-pages || true
if git -C "${REPO_ROOT}" show-ref --verify --quiet refs/heads/gh-pages; then
  git -C "${REPO_ROOT}" worktree add "${WORKTREE_DIR}" gh-pages
else
  git -C "${REPO_ROOT}" worktree add --detach "${WORKTREE_DIR}"
  git -C "${WORKTREE_DIR}" checkout --orphan gh-pages
  git -C "${WORKTREE_DIR}" rm -rf . || true
fi

cp -f "${PACKAGE_DIR}"/*.tgz "${WORKTREE_DIR}/"
if [[ -f "${WORKTREE_DIR}/index.yaml" ]]; then
  helm repo index "${WORKTREE_DIR}" --url "${PAGES_URL}" --merge "${WORKTREE_DIR}/index.yaml"
else
  helm repo index "${WORKTREE_DIR}" --url "${PAGES_URL}"
fi

git -C "${WORKTREE_DIR}" add .
git -C "${WORKTREE_DIR}" commit -m "Publish Helm chart ${VERSION}" || true
git -C "${WORKTREE_DIR}" push origin gh-pages

git -C "${REPO_ROOT}" worktree remove "${WORKTREE_DIR}" --force

echo "Released Helm chart version ${VERSION}"
