#!/usr/bin/env bash
# /*
# Copyright 2026 The Grove Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# */

set -o errexit
set -o nounset
set -o pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
MODULE_ROOT="$(dirname "$SCRIPT_DIR")"
VERSION=${VERSION:-$(cat "${MODULE_ROOT}/VERSION")}
GOARCH=${GOARCH:-}
PLATFORM=${PLATFORM:-}

CLUSTER_NAME=${CLUSTER_NAME:-}
KUBE_CONTEXT=${KUBE_CONTEXT:-}
NAMESPACE=${NAMESPACE:-grove}
DEPLOYMENT_NAME=${DEPLOYMENT_NAME:-grove-operator}
OPERATOR_CONTAINER=${OPERATOR_CONTAINER:-grove-operator}
PLACEHOLDER_IMAGE=${GROVE_SPREAD_PLACEHOLDER_IMAGE:-registry.k8s.io/pause:latest}
PLACEHOLDER_PRIORITY_CLASS_NAME=${GROVE_SPREAD_PLACEHOLDER_PRIORITY_CLASS_NAME:-grove-spread-placeholder-low}
WAIT_TIMEOUT=${WAIT_TIMEOUT:-180s}
SKIP_BUILD=false
LOAD_PLACEHOLDER_IMAGE=true
USE_HELM=true

if [[ -n "${REGISTRY:-}" ]]; then
  INITC_IMAGE="${REGISTRY}/grove-initc"
  OPERATOR_IMAGE="${REGISTRY}/grove-operator"
  INSTALL_CRDS_IMAGE="${REGISTRY}/grove-install-crds"
else
  INITC_IMAGE="grove-initc"
  OPERATOR_IMAGE="grove-operator"
  INSTALL_CRDS_IMAGE="grove-install-crds"
fi

INITC_IMAGE_REPOSITORY=${GROVE_INIT_CONTAINER_IMAGE:-${INITC_IMAGE}}
INITC_IMAGE_REF=${INITC_IMAGE}:${VERSION}
OPERATOR_IMAGE_REF=${OPERATOR_IMAGE_REF:-${OPERATOR_IMAGE}:${VERSION}}
INSTALL_CRDS_IMAGE_REF=${INSTALL_CRDS_IMAGE_REF:-${INSTALL_CRDS_IMAGE}:${VERSION}}

USAGE=""

function create_usage() {
  usage=$(printf '%s\n' "
  usage: $(basename "$0") [Options]

  Builds the Grove dev images, loads them into a kind cluster, refreshes the
  PodCliqueSet CRD, and updates the running grove-operator for spread-topology
  placeholder pods.

  Options:
    -n | --cluster-name <name>       Kind cluster name. Defaults to the current kind context.
    -c | --context <context>         Kubernetes context. Defaults to kind-<cluster-name>.
    --namespace <namespace>          Grove operator namespace. Default: grove.
    --skip-build                     Skip make docker-build and only load existing local images.
    --skip-placeholder-image         Do not pull/load the configured placeholder image.
    --no-helm                        Do not use helm upgrade; patch the Deployment directly.
    -h | --help                      Show this help.

  Environment:
    VERSION                          Image tag. Default: contents of VERSION.
    PLATFORM                         Docker platform. Default: linux/\$(go env GOARCH).
    REGISTRY                         Optional image registry prefix.
    GROVE_INIT_CONTAINER_IMAGE       Init container image repository, without a tag.
                                     Default: grove-initc.
    GROVE_SPREAD_PLACEHOLDER_IMAGE   Placeholder image. Default: registry.k8s.io/pause:latest.
    GROVE_SPREAD_PLACEHOLDER_PRIORITY_CLASS_NAME
                                     Placeholder PriorityClass. Default: grove-spread-placeholder-low.
  ")
  echo "${usage}"
}

function check_prerequisites() {
  for command in docker kind kubectl make go; do
    if ! command -v "${command}" &> /dev/null; then
      echo "${command} is not installed or is not on PATH."
      exit 1
    fi
  done
}

function parse_flags() {
  while test $# -gt 0; do
    case "$1" in
      --cluster-name | -n)
        shift
        CLUSTER_NAME=$1
        ;;
      --context | -c)
        shift
        KUBE_CONTEXT=$1
        ;;
      --namespace)
        shift
        NAMESPACE=$1
        ;;
      --skip-build)
        SKIP_BUILD=true
        ;;
      --skip-placeholder-image)
        LOAD_PLACEHOLDER_IMAGE=false
        ;;
      --no-helm)
        USE_HELM=false
        ;;
      -h | --help)
        echo "${USAGE}"
        exit 0
        ;;
      *)
        echo "Unknown flag: $1"
        echo "${USAGE}"
        exit 1
        ;;
    esac
    shift
  done
}

function infer_cluster_name() {
  if [[ -n "${CLUSTER_NAME}" ]]; then
    return
  fi

  local current_context
  current_context=$(kubectl config current-context 2> /dev/null || true)
  if [[ "${current_context}" == kind-* ]]; then
    CLUSTER_NAME="${current_context#kind-}"
    return
  fi

  local clusters
  mapfile -t clusters < <(kind get clusters 2> /dev/null || true)
  if [[ ${#clusters[@]} -eq 1 ]]; then
    CLUSTER_NAME="${clusters[0]}"
    return
  fi

  echo "Could not infer a kind cluster. Pass --cluster-name."
  exit 1
}

function configure_context() {
  infer_cluster_name
  if [[ -z "${KUBE_CONTEXT}" ]]; then
    KUBE_CONTEXT="kind-${CLUSTER_NAME}"
  fi
  KUBECTL=(kubectl --context "${KUBE_CONTEXT}")
}

function configure_build_defaults() {
  if [[ -z "${GOARCH}" ]]; then
    GOARCH=$(go env GOARCH)
  fi
  if [[ -z "${PLATFORM}" ]]; then
    PLATFORM="linux/${GOARCH}"
  fi
}

function validate_init_container_image_repository() {
  local image_name
  image_name="${INITC_IMAGE_REPOSITORY##*/}"
  if [[ "${image_name}" == *:* || "${INITC_IMAGE_REPOSITORY}" == *@* ]]; then
    echo "GROVE_INIT_CONTAINER_IMAGE must be an image repository without a tag or digest."
    echo "The operator appends VERSION internally; use '${INITC_IMAGE}' instead of '${INITC_IMAGE}:${VERSION}'."
    exit 1
  fi
}

function build_images() {
  if [[ "${SKIP_BUILD}" == true ]]; then
    echo "Skipping image build."
    return
  fi
  echo "Building Grove images for ${PLATFORM} with VERSION=${VERSION}..."
  VERSION="${VERSION}" PLATFORM="${PLATFORM}" make -C "${MODULE_ROOT}" docker-build
}

function append_image_once() {
  local image=$1
  for existing in "${KIND_LOAD_IMAGES[@]}"; do
    if [[ "${existing}" == "${image}" ]]; then
      return
    fi
  done
  KIND_LOAD_IMAGES+=("${image}")
}

function load_grove_images() {
  KIND_LOAD_IMAGES=()
  append_image_once "${INITC_IMAGE}:latest"
  append_image_once "${INITC_IMAGE_REF}"
  append_image_once "${OPERATOR_IMAGE}:latest"
  append_image_once "${OPERATOR_IMAGE_REF}"
  append_image_once "${INSTALL_CRDS_IMAGE}:latest"
  append_image_once "${INSTALL_CRDS_IMAGE_REF}"

  echo "Loading Grove images into kind cluster ${CLUSTER_NAME}..."
  kind load docker-image "${KIND_LOAD_IMAGES[@]}" --name "${CLUSTER_NAME}"
}

function load_placeholder_image() {
  if [[ "${LOAD_PLACEHOLDER_IMAGE}" != true ]]; then
    echo "Skipping placeholder image load."
    return
  fi

  if ! docker image inspect "${PLACEHOLDER_IMAGE}" &> /dev/null; then
    echo "Pulling placeholder image ${PLACEHOLDER_IMAGE}..."
    docker pull "${PLACEHOLDER_IMAGE}"
  fi
  echo "Loading placeholder image ${PLACEHOLDER_IMAGE} into kind cluster ${CLUSTER_NAME}..."
  kind load docker-image "${PLACEHOLDER_IMAGE}" --name "${CLUSTER_NAME}"
}

function apply_podcliqueset_crd() {
  local crd_path="${MODULE_ROOT}/api/core/v1alpha1/crds/grove.io_podcliquesets.yaml"
  echo "Updating podcliquesets.grove.io CRD..."
  if "${KUBECTL[@]}" get crd podcliquesets.grove.io &> /dev/null; then
    "${KUBECTL[@]}" replace -f "${crd_path}"
  else
    "${KUBECTL[@]}" apply -f "${crd_path}"
  fi

  local spread_type
  spread_type=$("${KUBECTL[@]}" get crd podcliquesets.grove.io -o jsonpath='{.spec.versions[?(@.name=="v1alpha1")].schema.openAPIV3Schema.properties.spec.properties.template.properties.cliques.items.properties.topologyConstraint.properties.spread.type}')
  if [[ "${spread_type}" != "boolean" ]]; then
    echo "PodCliqueSet CRD was updated, but topologyConstraint.spread is not present."
    exit 1
  fi
}

function apply_placeholder_priority_class() {
  echo "Ensuring placeholder PriorityClass ${PLACEHOLDER_PRIORITY_CLASS_NAME} exists..."
  "${KUBECTL[@]}" apply -f - <<EOF
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: ${PLACEHOLDER_PRIORITY_CLASS_NAME}
value: -100000
globalDefault: false
description: Low priority for Grove spread-topology placeholder pods.
EOF
}

function helm_release_name() {
  "${KUBECTL[@]}" -n "${NAMESPACE}" get deployment "${DEPLOYMENT_NAME}" -o jsonpath='{.metadata.annotations.meta\.helm\.sh/release-name}' 2> /dev/null || true
}

function upgrade_with_helm() {
  if [[ "${USE_HELM}" != true ]]; then
    return 1
  fi
  if ! command -v helm &> /dev/null; then
    return 1
  fi

  local release_name
  release_name=$(helm_release_name)
  if [[ -z "${release_name}" ]]; then
    return 1
  fi

  echo "Upgrading Helm release ${release_name} in namespace ${NAMESPACE}..."
  helm --kube-context "${KUBE_CONTEXT}" upgrade "${release_name}" "${MODULE_ROOT}/charts" \
    --namespace "${NAMESPACE}" \
    --reuse-values \
    --set config.topologyAwareScheduling.enabled=true \
    --set-string image.repository="${OPERATOR_IMAGE}" \
    --set-string image.tag="${VERSION}" \
    --set-string image.pullPolicy=IfNotPresent \
    --set-string "deployment.env[0].name=GROVE_INIT_CONTAINER_IMAGE" \
    --set-string "deployment.env[0].value=${INITC_IMAGE_REPOSITORY}" \
    --set-string "deployment.env[1].name=GROVE_SPREAD_PLACEHOLDER_IMAGE" \
    --set-string "deployment.env[1].value=${PLACEHOLDER_IMAGE}" \
    --set-string "deployment.env[2].name=GROVE_SPREAD_PLACEHOLDER_PRIORITY_CLASS_NAME" \
    --set-string "deployment.env[2].value=${PLACEHOLDER_PRIORITY_CLASS_NAME}"
}

function patch_deployment_directly() {
  echo "Patching Deployment ${NAMESPACE}/${DEPLOYMENT_NAME} directly..."
  "${KUBECTL[@]}" -n "${NAMESPACE}" set image "deployment/${DEPLOYMENT_NAME}" "${OPERATOR_CONTAINER}=${OPERATOR_IMAGE_REF}"
  "${KUBECTL[@]}" -n "${NAMESPACE}" set env "deployment/${DEPLOYMENT_NAME}" \
    "GROVE_INIT_CONTAINER_IMAGE=${INITC_IMAGE_REPOSITORY}" \
    "GROVE_SPREAD_PLACEHOLDER_IMAGE=${PLACEHOLDER_IMAGE}" \
    "GROVE_SPREAD_PLACEHOLDER_PRIORITY_CLASS_NAME=${PLACEHOLDER_PRIORITY_CLASS_NAME}"
}

function topology_aware_scheduling_enabled() {
  if "${KUBECTL[@]}" -n "${NAMESPACE}" get configmap -l app.kubernetes.io/component=operator-configmap -o yaml \
    | grep -A2 'topologyAwareScheduling:' \
    | grep -q 'enabled: true'; then
    return 0
  fi
  return 1
}

function ensure_topology_aware_scheduling_enabled() {
  if topology_aware_scheduling_enabled; then
    return
  fi
  echo "topologyAwareScheduling.enabled is not true in the operator config."
  echo "Install/upgrade the Helm release with config.topologyAwareScheduling.enabled=true, then rerun this script."
  exit 1
}

function update_operator_deployment() {
  if topology_aware_scheduling_enabled; then
    patch_deployment_directly
    return
  fi
  if ! upgrade_with_helm; then
    patch_deployment_directly
  fi
}

function restart_operator() {
  echo "Restarting grove-operator..."
  "${KUBECTL[@]}" -n "${NAMESPACE}" rollout restart "deployment/${DEPLOYMENT_NAME}"
  "${KUBECTL[@]}" -n "${NAMESPACE}" rollout status "deployment/${DEPLOYMENT_NAME}" --timeout="${WAIT_TIMEOUT}"
}

function print_summary() {
  echo
  echo "Updated ${DEPLOYMENT_NAME} in context ${KUBE_CONTEXT}."
  echo "  operator image: ${OPERATOR_IMAGE_REF}"
  echo "  init container image repository: ${INITC_IMAGE_REPOSITORY}"
  echo "  placeholder image: ${PLACEHOLDER_IMAGE}"
  echo "  placeholder priority class: ${PLACEHOLDER_PRIORITY_CLASS_NAME}"
  echo
  echo "Fabric labels currently visible on nodes:"
  "${KUBECTL[@]}" get nodes -L node.kubernetes.io/fabric-pod
}

function main() {
  check_prerequisites
  parse_flags "$@"
  configure_context
  configure_build_defaults
  validate_init_container_image_repository
  build_images
  load_grove_images
  load_placeholder_image
  apply_podcliqueset_crd
  apply_placeholder_priority_class
  update_operator_deployment
  restart_operator
  ensure_topology_aware_scheduling_enabled
  print_summary
}

USAGE=$(create_usage)
main "$@"
