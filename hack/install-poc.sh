#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Deploys the substrate-poc-3 demos onto a cluster that already runs Agent
# Substrate (install it first: ./hack/install-ate.sh --deploy-ate-system in
# the substrate repo). Modeled directly on substrate's hack/install-ate.sh
# so the demos can migrate into that repo with minimal changes: the same
# ATE_DEMOS registration pattern, the same helper names, the same sourced
# per-demo install scripts.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# Source the environment variables if configured
if [[ -f .poc-dev-env.sh ]] && [[ -z "${NO_DEV_ENV:-}" ]]; then
  source .poc-dev-env.sh
fi

# If the user has set KUBECTL_CONTEXT, we can assume they already have credentials.
if [[ -z "${KUBECTL_CONTEXT:-}" ]]; then
  # If PROJECT_ID is set, ensure kubeconfig is configured before running any kubectl commands.
  if [[ -n "${PROJECT_ID:-}" ]]; then
    gcloud container clusters get-credentials "${CLUSTER_NAME}" --location "${CLUSTER_LOCATION}" --project="${PROJECT_ID}"
  fi
fi
# otherwise just use the current cluster in KUBECONFIG ...

# ATE_DEMOS is an array that registers the prefix name of the demo functions.
ATE_DEMOS=()

# Include demos.
source "${ROOT}"/hack/install-demo-loopback-survival.sh
source "${ROOT}"/hack/install-demo-adk-calc.sh

# ANSI color codes for prettier output
COLOR_CYAN='\033[1;36m'
COLOR_RESET='\033[0m'

function log_step() {
  local step_name="$1"
  echo -e "${COLOR_CYAN}[step]: ${step_name}${COLOR_RESET}"
}

# --- Helper Functions ---
function usage() {
  echo "Usage: $0 [options]"
  echo ""
  echo "  --delete-all                           Delete all registered demos"
  echo ""
  for demo_name in "${ATE_DEMOS[@]}"; do
    echo "Demo: ${demo_name}"
    echo ""
    echo "  --deploy-${demo_name}                         Deploy ${demo_name}"
    echo "  --delete-${demo_name}                         Delete ${demo_name}"
    if declare -F "${demo_name}_usage" >/dev/null 2>&1; then
      "${demo_name}_usage"
    fi
  done
}

run_kubectl() {
  kubectl \
    ${KUBECTL_CONTEXT:+--context=${KUBECTL_CONTEXT}} \
    "$@"
}

# Unlike substrate's install-ate.sh (which does `go run ./cmd/kubectl-ate`
# from inside the substrate repo), this repo relies on the kubectl-ate
# plugin being installed: `go install ./cmd/kubectl-ate` in the substrate
# repo checkout.
run_kubectl_ate() {
  kubectl ate \
    ${KUBECTL_CONTEXT:+--context=${KUBECTL_CONTEXT}} \
    "$@"
}

run_ko() {
  # Only ko subcommands that delegate to kubectl (apply, create, delete, run)
  # accept args after `--`. ko build, resolve, deps, login etc. reject
  # `--context=...` as an unknown subcommand and abort the install.
  case "${1:-}" in
    apply|create|delete|run)
      ko "$@" \
          ${KUBECTL_CONTEXT:+-- --context="${KUBECTL_CONTEXT}"}
      ;;
    *)
      ko "$@"
      ;;
  esac
}

# ensure_crds verifies the substrate CRDs are present. This repo never
# installs them — Agent Substrate owns them (deploy with
# `./hack/install-ate.sh --deploy-ate-system` in the substrate repo).
ensure_crds() {
  log_step "ensure_crds"
  if run_kubectl get crd workerpools.ate.dev actortemplates.ate.dev sandboxconfigs.ate.dev >/dev/null 2>&1; then
    return
  fi
  echo "Error: Agent Substrate CRDs not found in the cluster." >&2
  echo "Install Agent Substrate first: ./hack/install-ate.sh --deploy-ate-system (substrate repo)." >&2
  exit 1
}

# ensure_env_vars fails fast when a template substitution variable is unset.
ensure_env_vars() {
  local missing=0
  for var in "$@"; do
    if [[ -z "${!var:-}" ]]; then
      echo "Error: ${var} must be set (see hack/poc-dev-env.sh.example)" >&2
      missing=1
    fi
  done
  ((missing == 0)) || exit 1
}

# get_actor_status echoes the actor's status enum (e.g. STATUS_SUSPENDED).
get_actor_status() {
  local actor_id="$1"
  local atespace="$2"
  local json

  if ! json=$(run_kubectl_ate get actor "${actor_id}" -a "${atespace}" -o json 2>/dev/null); then
    return 1
  fi
  jq -r '.actors[0].status // empty' <<<"${json}"
}

# prepare_actor_for_delete suspends (or resumes then suspends) until DeleteActor
# is allowed. Actors must be STATUS_SUSPENDED before deletion.
prepare_actor_for_delete() {
  local actor_id="$1"
  local atespace="$2"
  local timeout_secs="${3:-120}"
  local deadline=$((SECONDS + timeout_secs))
  local status

  while ((SECONDS < deadline)); do
    if ! status=$(get_actor_status "${actor_id}" "${atespace}"); then
      return 0
    fi

    case "${status}" in
      STATUS_SUSPENDED)
        return 0
        ;;
      STATUS_PAUSED)
        run_kubectl_ate resume actor "${actor_id}" -a "${atespace}" -o json >/dev/null
        ;;
      STATUS_RUNNING)
        run_kubectl_ate suspend actor "${actor_id}" -a "${atespace}" -o json >/dev/null
        ;;
      STATUS_RESUMING | STATUS_SUSPENDING | STATUS_PAUSING)
        ;;
      *)
        echo "cannot delete actor ${actor_id}: unexpected status ${status}" >&2
        return 1
        ;;
    esac
    sleep 2
  done

  echo "timed out waiting for actor ${actor_id} to reach STATUS_SUSPENDED" >&2
  return 1
}

# delete_demo_actors removes all actors for one or more (namespace, template)
# pairs before the demo manifests are deleted. Arguments are alternating
# namespace and template name, e.g.:
#   delete_demo_actors ate-demo-loopback-survival loopback-survival
delete_demo_actors() {
  if ! command -v jq &>/dev/null; then
    echo "jq is required to delete demo actors" >&2
    return 1
  fi

  if (($# == 0 || $# % 2 != 0)); then
    echo "delete_demo_actors expects namespace/template pairs" >&2
    return 1
  fi

  if ! run_kubectl get deployment/ate-api-server -n ate-system >/dev/null 2>&1; then
    log_step "ate-api-server not found; skipping actor cleanup"
    return 0
  fi

  local actors_json
  if ! actors_json=$(run_kubectl_ate get actors -A -o json 2>/dev/null); then
    echo "warning: could not list actors; skipping actor cleanup" >&2
    return 0
  fi

  local ns tmpl atespace actor_name
  while (($# > 0)); do
    ns="$1"
    tmpl="$2"
    shift 2

    log_step "Deleting actors for ${ns}/${tmpl}"
    while IFS=$'\t' read -r atespace actor_name; do
      [[ -z "${actor_name}" ]] && continue
      log_step "  preparing actor ${atespace}/${actor_name} for delete"
      prepare_actor_for_delete "${actor_name}" "${atespace}"
      run_kubectl_ate delete actor "${actor_name}" -a "${atespace}"
    done < <(
      jq -r --arg ns "${ns}" --arg tmpl "${tmpl}" \
        '.actors[]? | select(.actorTemplateNamespace == $ns and .actorTemplateName == $tmpl) | "\(.metadata.atespace)\t\(.metadata.name)"' \
        <<<"${actors_json}"
    )
  done
}

delete_all() {
  log_step "delete_all"
  for demo_name in "${ATE_DEMOS[@]}"; do
    if declare -F "${demo_name}_delete" >/dev/null 2>&1; then
      "${demo_name}_delete"
    fi
  done
}

if [ "$#" -eq 0 ]; then
  usage
  exit 1
fi

# If -h or --help appears anywhere in the command line, print the usage and exit.
for arg in "$@"; do
  case "$arg" in
    -h|--help)
      usage
      exit 0
      ;;
  esac
done

while [[ "$#" -gt 0 ]]; do
  # Run ${demo}_cmdline if it exists. If it returns 0, then we successfully
  # handled this argument and can continue. Otherwise, fallthrough to check
  # the other arguments.
  for demo_name in "${ATE_DEMOS[@]}"; do
    if declare -F "${demo_name}_cmdline" >/dev/null 2>&1; then
      if "${demo_name}_cmdline" "$1"; then
        shift
        continue 2
      fi
    fi
  done

  case $1 in
    --delete-all) delete_all ;;

    *)
      # Invalid option, should usage and exit with an error.
      echo "Error: unknown option: $1" >&2
      echo ""
      usage
      exit 1
      ;;
  esac
  shift
done
