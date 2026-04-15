#!/usr/bin/env bash
# Copyright 2025 Janos Czentye
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at:
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -eou pipefail

# Config ---------------------------------------------------------------------------------------------------------------

# Dependencies
DEPS=(docker k3d kubectl)

DOCKER_VER=28.5.2
K3D_VER=v5.8.3
KUBECTL_VER=v1.31.5	# used by k3d v5.8.3 / k3s v1.31.5

NO_CHECK=false
SLIM_SETUP=false
UPDATE=false

DOCKER_CHECK_IMG="hello-world:latest"
TEST_CLUSTER_NAME="test"
TEST_TIMEOUT=60

# Install actions ------------------------------------------------------------------------------------------------------

function install_deps() {
	printf "\n>>> Install dependencies...\n"
	sudo apt-get update && sudo apt-get install -y apt-transport-https ca-certificates curl \
	                                                gnupg-agent software-properties-common bash-completion
}

function install_docker() {
    if command -v docker >/dev/null 2>&1 && [ ${UPDATE} = true ]; then
        printf "\n>>> Remove previous %s...\n" "$(docker -v)"
        sudo systemctl stop docker
        sudo apt-mark unhold docker-ce docker-ce-cli docker-ce-rootless-extras
        sudo apt-get remove -y --allow-change-held-packages docker-ce docker-ce-cli containerd.io \
                            docker-compose-plugin docker-ce-rootless-extras docker-buildx-plugin docker-model-plugin
    fi
	echo -e "\n>>> Install Docker[${DOCKER_VER}]...\n"
	curl -fsSL https://get.docker.com/ | VERSION=${DOCKER_VER} sh
    # Privileged Docker
    sudo usermod -aG docker "${USER}"
	echo
	(set -x; docker --version)
}

function install_k3d() {
	printf "\n>>> Install k3d binary[%s]...\n" "${K3D_VER}"
	curl -fsSL https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | TAG=${K3D_VER} bash
	echo
	(set -x; k3d version)
}

function install_kubectl() {
	printf "\n>>> Install kubectl binary[%s]...\n" "${KUBECTL_VER}"
	curl -fsSL -O "https://dl.k8s.io/release/${KUBECTL_VER}/bin/linux/amd64/kubectl" && \
            sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl && \
            rm kubectl
	echo
	(set -x; kubectl version --client)
}

function setup_k3d_bash_completion() {
    printf "\n>>> Install k3d bash completion...\n"
    mkdir -p /etc/bash_completion.d
    k3d completion bash | sudo tee /etc/bash_completion.d/k3d > /dev/null
    sudo chmod a+r /etc/bash_completion.d/k3d
    source ~/.bashrc
    echo "Finished."
}

function setup_kubectl_bash_completion() {
    printf "\n>>> Install kubectl bash completion...\n"
    mkdir -p /etc/bash_completion.d
    kubectl completion bash | sudo tee /etc/bash_completion.d/kubectl > /dev/null
    sudo chmod a+r /etc/bash_completion.d/kubectl
    source ~/.bashrc
    echo "Finished."
}

function post_install() {
    echo -e "\n>>> Installed dependencies\n"
    docker --version
    k3d version
    kubectl version --client
}

# Test actions ---------------------------------------------------------------------------------------------------------

function create_test_cluster() {
    printf "\n>>> Prepare test cluster with id: %s...\n" "${TEST_CLUSTER_NAME}"
    k3d cluster create "${TEST_CLUSTER_NAME}" --wait --timeout="${TEST_TIMEOUT}s" --servers=1
    printf "\n>>> K3s setup:\n"
    kubectl version
    echo
    kubectl get all -A
}

function cleanup_test_cluster() {
	printf "\n>>> Cleanup...\n"
	k3d cluster delete "${TEST_CLUSTER_NAME}"
}

# Parameters -----------------------------------------------------------------------------------------------------------

function display_help() {
    cat <<EOF
Usage: ${0} [OPTIONS]

Options:
    -s  Only install minimum required binaries.
    -u  Update/overwrite dependencies.
    -x  Skip deployment validation.
    -h  Display help.

For further information, see https://github.com/hsnlab/edu-cloud-native/tree/main/k3d-setup
EOF
}

while getopts ":suxh" flag; do
	case "${flag}" in
        x)
            echo "[x] No setup validation is configured."
            NO_CHECK=true;;
        s)
            echo "[x] Slim install is configured."
            SLIM_SETUP=true;;
        u)
            echo "[x] Update dependencies."
            UPDATE=true;;
        h)
            display_help
            exit;;
        ?)
            echo "${0##*/}: invalid option -- '${OPTARG}'"
            echo "Try '${0} -h' for more information."
            exit 1;;
    esac
done

# Main -----------------------------------------------------------------------------------------------------------------

# Check for existence of ANY dependencies
if command -v "${DEPS[@]}" >/dev/null 2>&1 && [ "${UPDATE}" = false ]; then
    printf "\nSome of required dependencies are already installed, but the update flag (-u) is not set!\n\n"
    read -rp "Press ENTER to continue anyway or CTRL+C to abort..."
fi

### Basic dependencies
install_deps

### Docker
DOCKER_PRE_INSTALLED="$(command -pv docker)"
if ! command -v docker >/dev/null 2>&1 || [ "${UPDATE}" = true ]; then
    # Binary
	install_docker
    if [ ${NO_CHECK} = false ] && [ -z "${DOCKER_PRE_INSTALLED}" ]; then
        printf "\n>>> Jump into new shell for docker group privilege...\n" && sleep 3s
        # New shell with docker group privilege
        exec sg docker "$0" "$@"
    fi
    # Validation
    if [ ${NO_CHECK} = false ]; then
        printf "\n>>> Check Docker install...\n"
        # Docker check with simple container
        docker run --rm "${DOCKER_CHECK_IMG}" && docker rmi -f "${DOCKER_CHECK_IMG}"
    fi
fi

### K3d
if ! command -v k3d >/dev/null 2>&1 || [ "${UPDATE}" = true ]; then
	# Binary
	install_k3d
    if [ ${SLIM_SETUP} = false ]; then
        # Bash completion
        setup_k3d_bash_completion
    fi
fi

### Kubectl
if ! command -v kubectl >/dev/null 2>&1 || [ "${UPDATE}" = true ]; then
	# Binary
	install_kubectl
    if [ ${SLIM_SETUP} = false ]; then
        # Bash completion
        setup_kubectl_bash_completion
    fi
fi

# Register cleanup
trap cleanup_test_cluster ERR INT TERM

if [ ${NO_CHECK} = false ]; then
    # Make test
    create_test_cluster
    cleanup_test_cluster
    post_install
    # Make warning
    if [ -z "${DOCKER_PRE_INSTALLED}" ]; then
        cat <<EOF

#########################################################################################################
##  Shell session should be reloaded MANUALLY to make the non-root user access to Docker take effect!  ##
#########################################################################################################
EOF
    fi
fi

# Finish ---------------------------------------------------------------------------------------------------------------

echo -e "\nSetup is finished."
