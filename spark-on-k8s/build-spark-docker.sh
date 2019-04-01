#!/usr/bin/env bash

set -o nounset -o errexit -o pipefail

. utils.sh

CURR_DIR=$(pwd -P)
SPARK_VERSION="spark-2.4.0-bin-hadoop2.7"

function spark_exists {
  local base_dir="$@"
  echo "Checking if Spark artifacts exists in ${base_dir}..."
  dir_exists_or_err "$base_dir/jars"
  dir_exists_or_err "$base_dir/bin"
  dir_exists_or_err "$base_dir/sbin"
  dir_exists_or_err "$base_dir/data"
  dir_exists_or_err "$base_dir/kubernetes/tests"
  dir_exists_or_err "$base_dir/examples"
}

function download_if_not_exists {
  echo "Checking if Spark (${SPARK_VERSION}) has been downloaded to $CURR_DIR"
  local spark_base_dir="$CURR_DIR/$SPARK_VERSION"
  if dir_exists $spark_base_dir; then
    echo "It seems that Spark has been dowloaded already. Checking to make sure that necessary jars and scrips exist..."
  else
    echo "Spark has not been downloaded. Downloading Spark (${SPARK_VERSION})..."
    local spark_tar="${SPARK_VERSION}.tgz"
    # using `curl` over `wget` as MacOS is not shipped with `wget`
    curl -O "https://archive.apache.org/dist/spark/spark-2.4.0/$spark_tar"

    echo "Downloaded $SPARK_VERSION successfully. Untarring ${spark_tar}..."
    tar -xvzf $spark_tar
    rm $spark_tar
  fi
  spark_exists $spark_base_dir
}

function set_spark_home {
  if [ -z "${SPARK_HOME}" ]; then
    CURR_DIR
    SPARK_HOME="$CURR_DIR/$SPARK_VERSION/"
  fi
  . "${SPARK_HOME}/bin/load-spark-env.sh"
}

function image_ref {
  local image="$1"
  local add_repo="${2:-1}"
  if [ $add_repo = 1 ] && [ -n "$REPO" ]; then
    image="$REPO/$image"
  fi
  if [ -n "$TAG" ]; then
    image="$image:$TAG"
  fi
  echo "$image"
}

function build {

  local dependencies="$CURR_DIR/dependencies"
  if dir_exists $dependencies; then
    echo "$dependencies already exists."
  else
    echo "$dependencies does not exist. Creating..."
    mkdir $dependencies
  fi
  local build_args=("--build-arg" "spark_base=$SPARK_VERSION")
  echo "$build_args"
  echo "${build_args[@]}"

  echo "Checking if there is any Docker image with the tag ${TAG}..."
  local existing_images=$(docker images --filter=reference="*:$TAG" -q)
  if [ ! -z "$existing_images" ]; then
    echo "Removing the following Docker images with the tag ${TAG}: $existing_images"
    docker rmi -f $existing_images
  fi

  docker build $NOCACHEARG "${build_args[@]}" \
    -t $(image_ref spark) \
    -f "$CURR_DIR/Dockerfile" .
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
    file_exists_or_err "$CURR_DIR/Dockerfile"
    file_exists_or_err "$CURR_DIR/entrypoint.sh"
    set_spark_home
    build
    ;;
  push)
    if [ -z "$REPO" ]; then
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
