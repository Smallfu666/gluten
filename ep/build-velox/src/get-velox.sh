#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -exu

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
VELOX_REPO=https://github.com/IBM/velox.git
VELOX_BRANCH=dft-2026_07_28
VELOX_ENHANCED_BRANCH=ibm-2026_07_28
VELOX_HOME=""
RUN_SETUP_SCRIPT=ON
ENABLE_ENHANCED_FEATURES=OFF

# Developer use only for testing Velox PR.
UPSTREAM_VELOX_PR_ID=""

OS=`uname -s`

for arg in "$@"; do
  case $arg in
  --velox_repo=*)
    VELOX_REPO=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --velox_branch=*)
    VELOX_BRANCH=("${arg#*=}")
    VELOX_ENHANCED_BRANCH=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --velox_home=*)
    VELOX_HOME=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --run_setup_script=*)
    RUN_SETUP_SCRIPT=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_enhanced_features=*)
    ENABLE_ENHANCED_FEATURES=("${arg#*=}")
    VELOX_BRANCH=$VELOX_ENHANCED_BRANCH
    shift # Remove argument name from processing
    ;;
  *)
    OTHER_ARGUMENTS+=("$1")
    shift # Remove generic argument from processing
    ;;
  esac
done

if [ "$VELOX_HOME" == "" ]; then
  VELOX_HOME="$CURRENT_DIR/../build/velox_ep"
fi

function process_setup_ubuntu {
  sed -i "s|run_and_time install_arrow||g" scripts/setup-ubuntu.sh
  # GLUTEN 12316: Ubuntu's apt libgflags.a is not -fPIC, so linking it into the shared
  # libvelox.so fails (relocation R_X86_64_PC32 against `stderr`) whenever Folly is static
  # and exports gflags_static — i.e. the VELOX_BUILD_SHARED=OFF path. apt glog also drags
  # apt libgflags.so.2.2 into the process at runtime. Build the pinned gflags from source
  # with PIC and additionally run the canonical install_glog from setup-common.sh so glog is
  # source-built against the same gflags. Both install into /usr/local (searched before the
  # apt copies) and run before install_folly. cmake_install_dir forces -fPIC.
  #
  # Scope: only run the source builds when VELOX_BUILD_SHARED != ON. With ON, Folly is shared
  # and uses gflags_shared, so libvelox.so never links the non-PIC static archive and needs
  # no source gflags/glog. The install_gflags definition is always injected (harmless if the
  # call is skipped); the calls are gated. The gate variable is set by setup-common.sh
  # (sourced above by setup-ubuntu.sh), the same var its own install_folly branches on.
  if ! grep -q "^function install_gflags {" scripts/setup-ubuntu.sh; then
    sed -i '/function install_velox_deps {/i \
function install_gflags {\
  wget_and_untar https://github.com/gflags/gflags/archive/${GFLAGS_VERSION}.tar.gz gflags\
  cmake_install_dir gflags -DBUILD_SHARED_LIBS=ON -DBUILD_STATIC_LIBS=ON -DBUILD_gflags_LIB=ON\
}\
' scripts/setup-ubuntu.sh
  fi
  if ! grep -q "^[[:space:]]*run_and_time install_gflags$" scripts/setup-ubuntu.sh; then
    sed -i '/run_and_time install_velox_deps_from_apt/a \
  if [ "${VELOX_BUILD_SHARED:-OFF}" != "ON" ]; then\
    run_and_time install_gflags\
    run_and_time install_glog\
  fi' scripts/setup-ubuntu.sh
  fi
  local gflags_func gflags_call glog_call gate
  gflags_func=$(grep -c "^function install_gflags {" scripts/setup-ubuntu.sh || true)
  gflags_call=$(grep -c "^[[:space:]]*run_and_time install_gflags$" scripts/setup-ubuntu.sh || true)
  glog_call=$(grep -c "^[[:space:]]*run_and_time install_glog$" scripts/setup-ubuntu.sh || true)
  gate=$(grep -c 'if \[ "${VELOX_BUILD_SHARED:-OFF}" != "ON" \]; then' scripts/setup-ubuntu.sh || true)
  # install_glog is provided canonically by setup-common.sh (sourced by setup-ubuntu.sh);
  # only inject its call, and assert the canonical definition is present to resolve it.
  if [ "$gflags_func" -ne 1 ] || [ "$gflags_call" -ne 1 ] || [ "$glog_call" -ne 1 ] || [ "$gate" -ne 1 ] \
     || ! grep -q "^function install_glog {" scripts/setup-common.sh; then
    echo "ERROR: gflags/glog injection failed: gflags_func=$gflags_func gflags_call=$gflags_call glog_call=$glog_call gate=$gate canonical_install_glog=$(grep -c '^function install_glog {' scripts/setup-common.sh)"; exit 1
  fi
  bash -n scripts/setup-ubuntu.sh || { echo "ERROR: setup-ubuntu.sh syntax check failed"; exit 1; }
  echo "Using setup script from Velox"
}

function process_setup_centos9 {
  sed -i "s|run_and_time install_arrow||g" scripts/setup-centos9.sh
  echo "Using setup script from Velox"
}

function process_setup_alinux3 {
  sed -i "/^[[:space:]]*#/!s/.*dnf_install epel-release/#&/" ${CURRENT_DIR}/setup-centos8.sh
  sed -i "/^[[:space:]]*#/!s/.*run_and_time install_conda/#&/" ${CURRENT_DIR}/setup-centos8.sh
  sed -i "/^[[:space:]]*#/!s/.*dnf config-manager --set-enabled powertools/#&/" ${CURRENT_DIR}/setup-centos8.sh
  sed -i "s/gcc-toolset-11 //" ${CURRENT_DIR}/setup-centos8.sh
  sed -i "/^[[:space:]]*#/!s/.*source \/opt\/rh\/gcc-toolset-11\/enable/#&/" ${CURRENT_DIR}/setup-centos8.sh
  sed -i "/^[[:space:]]*#/!s|^export CC=/opt/rh/gcc-toolset-11/root/bin/gcc|# &|" ${CURRENT_DIR}/setup-centos8.sh
  sed -i "/^[[:space:]]*#/!s|^export CXX=/opt/rh/gcc-toolset-11/root/bin/g++|# &|" ${CURRENT_DIR}/setup-centos8.sh
  sed -i 's/python39 python39-devel python39-pip //g' ${CURRENT_DIR}/setup-centos8.sh
  sed -i "/^[[:space:]]*#/!s/.*pip.* install/#&/" ${CURRENT_DIR}/setup-centos8.sh
}

function process_setup_tencentos32 {
  sed -i "/^[[:space:]]*#/!s/.*dnf config-manager --set-enabled powertools/#&/" ${CURRENT_DIR}/setup-centos8.sh
}

# Folly enables jemalloc when Homebrew headers are visible but does not link
# libjemalloc, leaving _mallocx/_nallocx undefined for downstream links. Keep
# Folly's Linux-equivalent no-jemalloc behavior; Gluten's own jemalloc build is
# independent of this. Header-search isolation from /usr/local is handled by
# SDKROOT exported in builddeps-veloxbe.sh / build-velox.sh.
function process_setup_macos {
  if ! grep -Fq 'FOLLY_USE_JEMALLOC=OFF' scripts/setup-common.sh; then
    sed -i '' 's/local FOLLY_FLAGS=(/local FOLLY_FLAGS=(-DFOLLY_USE_JEMALLOC=OFF /' scripts/setup-common.sh
  fi
}

function prepare_velox_source_code {
  echo "Preparing Velox source code..."

  # checkout code
  TARGET_BUILD_COMMIT="$(git ls-remote $VELOX_REPO $VELOX_BRANCH | awk '{print $1;}' | head -n 1)"
  if [ -d $VELOX_HOME ]; then
    echo "Velox source folder $VELOX_HOME already exists..."
    cd $VELOX_HOME
    # if velox_branch exists, check it out,
    # otherwise assume that user prepared velox source in velox_home, skip checkout
    if [ -n "$TARGET_BUILD_COMMIT" ]; then
      git init .
      EXISTS=$(git show-ref refs/tags/build_$TARGET_BUILD_COMMIT || true)
      if [ -z "$EXISTS" ]; then
        git fetch $VELOX_REPO $TARGET_BUILD_COMMIT:refs/tags/build_$TARGET_BUILD_COMMIT
      fi
      git reset --hard HEAD
      git checkout refs/tags/build_$TARGET_BUILD_COMMIT
    else
      echo "$VELOX_BRANCH can't be found in $VELOX_REPO, skipping the download..."
    fi
  else
    git clone $VELOX_REPO -b $VELOX_BRANCH $VELOX_HOME
    cd $VELOX_HOME
    git checkout $TARGET_BUILD_COMMIT
  fi

  #sync submodules
  git submodule sync --recursive
  git submodule update --init --recursive
}

function apply_provided_velox_patch {
  if [[ -n "$UPSTREAM_VELOX_PR_ID" ]]; then
     echo "Applying patch for PR #$UPSTREAM_VELOX_PR_ID ..."
     local patch_name="$UPSTREAM_VELOX_PR_ID.patch"
     pushd $VELOX_HOME
     rm -f $patch_name
     wget -nv "https://patch-diff.githubusercontent.com/raw/facebookincubator/velox/pull/$UPSTREAM_VELOX_PR_ID.patch" \
       -O "$patch_name" || {
       echo "Failed to download the Velox patch from GitHub"
       exit 1
     }
     (git apply --check $patch_name && git apply $patch_name) || {
       echo "Failed to apply the provided Velox patch"
       exit 1
     }
     popd
  fi
}

function apply_compilation_fixes {
  local SUDO_CMD=""
  if [ "$OS" == "Linux" ]; then
    SUDO_CMD="sudo"
  fi
  $SUDO_CMD cp ${CURRENT_DIR}/modify_arrow.patch ${VELOX_HOME}/CMake/resolve_dependency_modules/arrow/

  git add ${VELOX_HOME}/CMake/resolve_dependency_modules/arrow/modify_arrow.patch # to avoid the file from being deleted by git clean -dffx :/
}

function setup_linux {
  local LINUX_DISTRIBUTION=$(. /etc/os-release && echo ${ID})
  local LINUX_VERSION_ID=$(. /etc/os-release && echo ${VERSION_ID})

  export SUDO="sudo --preserve-env"
  if [[ "$LINUX_DISTRIBUTION" == "ubuntu" || "$LINUX_DISTRIBUTION" == "debian" || "$LINUX_DISTRIBUTION" == "pop" ]]; then
    process_setup_ubuntu
  elif [[ "$LINUX_DISTRIBUTION" == "centos" ]]; then
    case "$LINUX_VERSION_ID" in
      9) process_setup_centos9 ;;
      8) ;;
      7) ;;
      *)
        echo "Unsupported centos version: $LINUX_VERSION_ID"
        exit 1
      ;;
    esac
  elif [[ "$LINUX_DISTRIBUTION" == "openEuler" ]]; then
    # this is workaround for gcc-12.3.1
    # https://github.com/facebookincubator/velox/blob/b263d9dd8b8910dc642d8fdb0c0adee4b2a1fb29/CMakeLists.txt#L433
    sed -i "s|no-unknown-warning-option|no-unknown-warning-option -Wno-restrict|g" ../../src/build-velox.sh
    case "$LINUX_VERSION_ID" in
      24.03) ;;
      *)
        echo "Unsupported openEuler version: $LINUX_VERSION_ID"
        exit 1
      ;;
    esac
  elif [[ "$LINUX_DISTRIBUTION" == "alinux" ]]; then
    case "${LINUX_VERSION_ID:0:1}" in
      2) ;;
      3) process_setup_alinux3 ;;
      *)
        echo "Unsupported alinux version: $LINUX_VERSION_ID"
        exit 1
      ;;
    esac
  elif [[ "$LINUX_DISTRIBUTION" == "tencentos" ]]; then
    case "$LINUX_VERSION_ID" in
      2.4) ;;
      3.2) process_setup_tencentos32 ;;
      *)
        echo "Unsupported tencentos version: $LINUX_VERSION_ID"
        exit 1
      ;;
    esac
  elif [[ "$LINUX_DISTRIBUTION" == "rhel" || "$LINUX_DISTRIBUTION" == "rocky" || \
    "$LINUX_DISTRIBUTION" == "almalinux" ]]; then
    case "${LINUX_VERSION_ID%%.*}" in
      9) ;;
      *)
        echo "Unsupported ${LINUX_DISTRIBUTION} version: $LINUX_VERSION_ID"
        exit 1
      ;;
    esac
  else
    echo "Unsupported linux distribution: $LINUX_DISTRIBUTION"
    exit 1
  fi
}

function apply_setup_fixes() {
  if [ $OS == 'Linux' ]; then
    setup_linux
  elif [ $OS == 'Darwin' ]; then
    process_setup_macos
  else
    echo "Unsupported kernel: $OS"
    exit 1
  fi
}

prepare_velox_source_code

if [[ "$RUN_SETUP_SCRIPT" == "ON" ]]; then
  apply_setup_fixes
fi

apply_provided_velox_patch

apply_compilation_fixes

echo "Finished getting Velox code"
