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
#

set -o errexit -o pipefail

CURR_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. ${CURR_DIR}/utils.sh

SCALA_VERSION="2.12"

DEPENDENCIES_PATH="${CURR_DIR}/dependencies"
SPARK_VERSION="3.0.0"
HADOOP_VERSION="2.7"
SPARK_BIN="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"
UBER_JAR="spark-cassandra.jar"

function check_requirements {
  command_exists_or_err sbt
  command_exists_or_err docker
}

function spark_exists {
  local base_dir="$@"
  echo "Checking if Spark artifacts exists in ${base_dir}..."
  dir_exists_or_err "${base_dir}/jars"
  dir_exists_or_err "${base_dir}/bin"
  dir_exists_or_err "${base_dir}/sbin"
  dir_exists_or_err "${base_dir}/data"
  dir_exists_or_err "${base_dir}/kubernetes/tests"
  dir_exists_or_err "${base_dir}/examples"
}

function download_if_not_exists {
  echo "Checking if Spark (${SPARK_BIN}) has been downloaded to ${CURR_DIR}"
  local spark_base_dir="${CURR_DIR}/${SPARK_BIN}"
  if dir_exists ${spark_base_dir}; then
    echo "It seems that Spark has been dowloaded already. Checking to make sure that necessary jars and scrips exist..."
  else
    echo "Spark has not been downloaded. Downloading Spark (${SPARK_BIN})..."
    local spark_tar="${SPARK_BIN}.tgz"
    # using `curl` over `wget` as MacOS is not shipped with `wget`
    curl -O "https://archive.apache.org/dist/spark/spark-3.0.0/${spark_tar}"

    echo "Downloaded ${SPARK_BIN} successfully. Untarring ${spark_tar}..."
    tar -xvzf $spark_tar
    rm $spark_tar
  fi
  spark_exists $spark_base_dir
}

function set_spark_home {
  if [ -z "${SPARK_HOME}" ]; then
    SPARK_HOME="${CURR_DIR}/${SPARK_BIN}/"
  fi
  . "${SPARK_HOME}/bin/load-spark-env.sh"
}

function compile_dependencies {
  local sbt_path="${CURR_DIR}/spark-cassandra/"
  cd ${sbt_path}
  echo "Compiling Spark Cassandra example SBT project..."
  sbt clean assembly
  echo "Successfully compiled Spark Cassandra example."
  cd ${CURR_DIR}

  local uber_jar_src_path="${sbt_path}/target/scala-${SCALA_VERSION}/${UBER_JAR}"
  local uber_jar_dest_path="${DEPENDENCIES_PATH}/${UBER_JAR}"
  echo "Copying the compiled uber jar from ${uber_jar_src_path} to ${uber_jar_dest_path}"
  cp ${uber_jar_src_path} ${uber_jar_dest_path}
}

function image_ref {
  local image="$1"
  local add_repo="${2:-1}"
  if [ $add_repo = 1 ] && [ -n "${REPO}" ]; then
    image="${REPO}/${image}"
  fi
  if [ -n "$TAG" ]; then
    image="${image}:${TAG}"
  fi
  echo "${image}"
}

function build {
  if dir_exists ${DEPENDENCIES_PATH}; then
    echo "${DEPENDENCIES_PATH} already exists."
  else
    echo "${DEPENDENCIES_PATH} does not exist. Creating..."
    mkdir ${DEPENDENCIES_PATH}
  fi
  compile_dependencies

  local build_args=("--build-arg" "spark_base=${SPARK_BIN}")

  echo "Checking if there is any Docker image with the tag ${TAG}..."
  local existing_images=$(docker images --filter=reference="*:${TAG}" -q)
  if [ ! -z "${existing_images}" ]; then
    echo "Removing the following Docker images with the tag ${TAG}: ${existing_images}"
    docker rmi -f ${existing_images}
  fi

  docker build ${NOCACHEARG} "${build_args[@]}" \
    -t $(image_ref spark) \
    -f "${CURR_DIR}/Dockerfile" .
}

function push {
  docker push "$(image_ref spark)"
}

function usage {
  cat <<EOF
Usage: $0 [options] [command]
Builds or pushes the built-in Spark Docker image.

Commands:
  build      Build image. Requires a repository address to be provided if the image will be
             pushed to a different registry.
  push       Push a pre-built image to a registry. Requires a repository address to be provided.

Options:
  -f file    Dockerfile to build for JVM based Jobs. By default builds the Dockerfile shipped with Spark.
  -r repo    Repository address.
  -t tag     Tag to apply to the built image, or to identify the image to be pushed.
  -n         Build docker image with --no-cache

Examples:
  - Build and push image with tag "v2.3.0" to docker.io/myrepo
    $0 -r docker.io/myrepo -t v2.3.0 build
    $0 -r docker.io/myrepo -t v2.3.0 push
EOF
}

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  usage
  exit 0
fi

REPO=
TAG=
BASEDOCKERFILE=
NOCACHEARG=
while getopts f:r:t:n option
do
  case "${option}" in
  f) BASEDOCKERFILE=${OPTARG};;
  r) REPO=${OPTARG};;
  t) TAG=${OPTARG};;
  n) NOCACHEARG="--no-cache";;
  esac
done

case "${@: -1}" in
  build)
    download_if_not_exists
    file_exists_or_err "${CURR_DIR}/Dockerfile"
    file_exists_or_err "${CURR_DIR}/entrypoint.sh"
    set_spark_home
    build
    ;;
  push)
    if [ -z "${REPO}" ]; then
      usage
      exit 1
    fi
    push
    ;;
  *)
    usage
    exit 1
    ;;
esac
