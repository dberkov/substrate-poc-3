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

ATE_DEMOS+=(demo-loopback-survival) # register demo-loopback-survival

demo-loopback-survival_cmdline() {
  case "${1}" in
    --deploy-demo-loopback-survival) demo-loopback-survival_deploy ;;
    --delete-demo-loopback-survival) demo-loopback-survival_delete ;;
    *)
      return 1
      ;;
  esac
  return 0
}

demo-loopback-survival_deploy() {
  log_step "demo-loopback-survival_deploy"
  ensure_crds
  ensure_env_vars BUCKET_NAME KO_DOCKER_REPO
  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
      demos/loopback-survival/loopback-survival.yaml.tmpl \
    | run_ko apply -f -

  # Wait for the demo to be fully ready before returning. The ActorTemplate
  # golden snapshot only becomes Ready once the client's /readyz reports the
  # loopback connection established — i.e. the golden snapshot already
  # contains an open connection.
  log_step "Waiting for loopback-survival demo to be ready..."
  run_kubectl rollout status deployment/externalecho -n ate-demo-loopback-survival --timeout=300s
  run_kubectl wait --for=condition=Ready actortemplate/loopback-survival -n ate-demo-loopback-survival --timeout=300s
}

demo-loopback-survival_delete() {
  log_step "demo-loopback-survival_delete"
  delete_demo_actors ate-demo-loopback-survival loopback-survival
  ensure_env_vars BUCKET_NAME
  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
      demos/loopback-survival/loopback-survival.yaml.tmpl \
    | run_kubectl delete --ignore-not-found -f -
}
