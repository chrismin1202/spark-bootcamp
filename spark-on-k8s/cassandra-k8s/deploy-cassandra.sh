#!/usr/bin/env bash

set -o nounset -o errexit -o pipefail

. ../utils.sh

function namespace_exists {
  # verify that the namespace exists
  local ns_exists=`kubectl get namespace $@ --no-headers --output=go-template={{.metadata.name}} 2>/dev/null`
  [ ! -z "${ns_exists}" ]
}

function check_requirements {
  command_exists_or_err kubectl
  command_exists_or_err helm
}

function deploy {
  check_requirements

  echo "Deploying Cassandra cluster named ${CLUSTER_NAME} to the namespace ${K8S_NAMESPACE}..."
  helm install \
    --namespace ${K8S_NAMESPACE} -n ${CLUSTER_NAME} \
    --set config.cluster_size=2,config.max_heap_size=1024M,config.heap_new_size=512M,persistence.size=1Gi,persistence.enabled=true \
    incubator/cassandra
}

function destory {
  check_requirements
  
  echo "Destorying Cassandra cluster named ${CLUSTER_NAME}..."
  helm delete  --purge ${CLUSTER_NAME}
}

DEFAULT_K8S_NAMESPACE="cassandra"
DEFAULT_CLUSTER_NAME="cassandra"

function usage {
  cat <<EOF
$0 deploys or destroys Cassandra cluster on Kubernetes.

Usage:  $0 [--deploy|--destroy]

Options:

  --help           This help.

  --deploy         Deploys Cassandra on Kubernetes via helm.

  --destroy        Destroys Cassandra Helm Chart.

  -ns|--namespace  The Kubernetes namespace to use (defalt: ${DEFAULT_K8S_NAMESPACE})

  -n|--name        The name of the Cassandra cluster (default: ${DEFAULT_CLUSTER_NAME})

Examples:

  - Deploys with the default name (${DEFAULT_CLUSTER_NAME}) to the default namespace (${DEFAULT_K8S_NAMESPACE}).
    sh $0 --deploy

  - Destroys the cluster with the default name (${DEFAULT_CLUSTER_NAME}).
    sh $0 --destroy

  - Deploys with the custom name to the custom namespace.
    sh $0 --deploy --namespace my-cassandra --name cassandra-cluster

  - Destroys the cluster with non-default name.
    sh $0 --destroy --name cassandra-cluster # destroys Cassandra cluster

EOF
}

PRINT_HELP=false
DEPLOY=false
DESTROY=false
K8S_NAMESPACE=${DEFAULT_K8S_NAMESPACE}
CLUSTER_NAME=${DEFAULT_CLUSTER_NAME}

# Note that getopt is not so MacOS-friendly.
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
  -h|--help)
    PRINT_HELP=true
    shift
    ;;
  --deploy)
    DEPLOY=true
    shift 
    ;;
  --destory)
    DESTROY=true
    shift 
    ;;
  -ns|--namespace)
    K8S_NAMESPACE="$2"
    shift 2
    ;;
  -n|--name)
    CLUSTER_NAME="$2"
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *) # unknown option
    echo "Unknow arg ${key}"
    shift
    ;;
esac
done

if "${PRINT_HELP}"; then 
  usage
  exit 0
fi

if "${DEPLOY}"; then 
  deploy
  exit 0
fi

if "${DESTROY}"; then 
  destory
  exit 0
fi
