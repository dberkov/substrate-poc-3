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
# This is sourced as part of install-poc.sh. Do not run directly.

ATE_DEMOS+=(demo-adk-calc) # register demo-adk-calc

# ATE_ATESPACE is the atespace actors are created in. It is baked into the
# sidecar's env (constant per template), so the client MUST create actors in
# the same atespace (the client defaults to "demo").
: "${ATE_ATESPACE:=demo}"

demo-adk-calc_usage() {
  echo "  --redeploy-egress-broker                      Rebuild + roll only the egress-broker Deployment"
  echo "  --redeploy-ingress-broker                     Rebuild + roll only the ingress-broker Deployment"
  echo "  --redeploy-mcp-server                         Rebuild + roll only the mcp-server Deployment"
}

demo-adk-calc_cmdline() {
  case "${1}" in
    --deploy-demo-adk-calc) demo-adk-calc_deploy ;;
    --delete-demo-adk-calc) demo-adk-calc_delete ;;
    --redeploy-egress-broker)
      demo-adk-calc_redeploy egress-broker github.com/dberkov/substrate-poc-3/cmd/egress-broker
      ;;
    --redeploy-ingress-broker)
      demo-adk-calc_redeploy ingress-broker github.com/dberkov/substrate-poc-3/demos/adk-calc/ingress-broker
      ;;
    --redeploy-mcp-server)
      demo-adk-calc_redeploy mcp-server github.com/dberkov/substrate-poc-3/demos/adk-calc/mcp-server
      ;;
    *)
      return 1
      ;;
  esac
  return 0
}

# demo-adk-calc_redeploy rebuilds one out-of-actor component's image with ko
# and rolls only its Deployment — much faster than --deploy-demo-adk-calc,
# which resolves every image in the template (including ateom-gvisor).
#
# Caveats:
#   - Restarting the egress-broker drops its in-memory sessions and the
#     upstream connections it is holding. Any in-flight tool call fails and
#     the agent retries; suspended actors with pending sessions are NOT
#     woken by the new broker instance (use /debug/resume or atenet). Fine
#     for dev iteration; do it while no calls are in flight.
#   - In-actor components (agent-server, egress-sidecar) are NOT handled
#     here: their images are pinned into the ActorTemplate golden snapshot,
#     so changing them requires --deploy-demo-adk-calc (and recreating
#     actors created from the old template).
demo-adk-calc_redeploy() {
  local name="$1" importpath="$2"
  log_step "demo-adk-calc_redeploy ${name}"
  ensure_env_vars KO_DOCKER_REPO

  local image
  image="$(run_ko build "${importpath}")"
  log_step "  built ${image}"

  local current
  current="$(run_kubectl get "deployment/${name}" -n ate-demo-adk-calc \
    -o jsonpath='{.spec.template.spec.containers[0].image}')"
  if [[ "${current}" == "${image}" ]]; then
    # Same digest (no code change): kubectl set image would be a no-op, so
    # force a bounce instead.
    log_step "  image unchanged; forcing rollout restart"
    run_kubectl rollout restart "deployment/${name}" -n ate-demo-adk-calc
  else
    run_kubectl set image "deployment/${name}" -n ate-demo-adk-calc "${name}=${image}"
  fi
  run_kubectl rollout status "deployment/${name}" -n ate-demo-adk-calc --timeout=300s
}

demo-adk-calc_render() {
  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
      -e "s|\${GOOGLE_API_KEY}|${GOOGLE_API_KEY}|g" \
      -e "s|\${ATE_ATESPACE}|${ATE_ATESPACE}|g" \
      demos/adk-calc/adk-calc.yaml.tmpl
}

demo-adk-calc_deploy() {
  log_step "demo-adk-calc_deploy"
  ensure_crds
  ensure_env_vars BUCKET_NAME GOOGLE_API_KEY KO_DOCKER_REPO

  # ActorTemplate spec is IMMUTABLE in substrate: `kubectl apply` of a changed
  # template (e.g. a new agent-server/egress-sidecar image digest after a code
  # change) is rejected. So delete the template (and its actors, which
  # reference it) first, letting apply recreate it and cut a fresh golden
  # snapshot from the current in-actor images. Out-of-actor Deployments are
  # unaffected — for those alone, use the faster --redeploy-* flags.
  delete_demo_actors ate-demo-adk-calc adk-calc
  run_kubectl delete actortemplate/adk-calc -n ate-demo-adk-calc --ignore-not-found

  demo-adk-calc_render | run_ko apply -f -

  log_step "Waiting for adk-calc demo to be ready..."
  run_kubectl rollout status deployment/mcp-server -n ate-demo-adk-calc --timeout=300s
  run_kubectl rollout status deployment/egress-broker -n ate-demo-adk-calc --timeout=300s
  run_kubectl rollout status deployment/ingress-broker -n ate-demo-adk-calc --timeout=300s
  run_kubectl wait --for=condition=Ready actortemplate/adk-calc -n ate-demo-adk-calc --timeout=300s
}

demo-adk-calc_delete() {
  log_step "demo-adk-calc_delete"
  delete_demo_actors ate-demo-adk-calc adk-calc
  ensure_env_vars BUCKET_NAME GOOGLE_API_KEY
  demo-adk-calc_render | run_kubectl delete --ignore-not-found -f -
}
