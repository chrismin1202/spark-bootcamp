# Spark on Kubernetes Example

## Overview
This example demonstrates running Spark on Kubernetes with Cassandra as the data source. You can adapt the sample Dockerfiles, YAML scripts, and sample code in this repository to build your own Spark-on-k8s application. Note that this is just a simple example I cobbled together for demo purpose. It is nowhere near production-ready.<br/><br/>
The Cassandra cluster used in this example is built with [Helm](https://helm.sh/), which is a package manager for Kubernetes. The script `deploy-cassandra.sh` dowloads [Cassandra Helm Chart](https://hub.helm.sh/charts/incubator/cassandra) from [Helm Hub](https://hub.helm.sh/). Note that Cassandra Chart is currently in incubator stage. If you are planning on using the chart for production, you probably need to pull the chart and improve upon it.

## Requirements

* Hardware (if running from a single machine)
  * 16GiB of system memory or more
  * 4 physical CPU cores or more
  * 4GB of storage
  * To run the example, Docker needs about 8GiB of memory and 2-4 CPU cores allocated.
* Scala Version: 2.11.12 (You can downgrade, but I recommend 2.11.8 or higher)
* SBT
  * Install SBT and have `sbt` command available.
* Docker
  * Install Docker and have `docker` command available.
* Kubernetes
  * You can either enable Kubernetes in Docker Desktop or use Minikube. You need to have `kubectl` command available.
* Helm
  * Helm is a package manager for Kubernetes. It facilitates deployment of Kubernetes applications.
  * Install it and have `helm` command available.

## Setup

1. Install Docker.<br/>
   If you are on macOS or Windows, [Docker Desktop](https://www.docker.com/products/docker-desktop) is probably the easiest way to run Docker. If you are running from a Linux server, refer to [Official Docker Documentation](https://docs.docker.com), e.g., [installing on CentOS](https://docs.docker.com/install/linux/docker-ce/centos/).
1. Install Kubernetes.<br/>
   If you opted for Docker Desktop, you can simply enable Kubernetes from it. Note that if you use Minikube alongside Docker Desktop with Kubernetes enabled, you can run into conflicts. 
1. Install Helm.<br/>
   Refer to [Helm Documentation](https://helm.sh/docs/using_helm/#installing-the-helm-client) for installation instruction.<br/>
   I have installed on macOS and CentOS. On macOS, Homebrew works brilliantly. On CentOS, on the other hand, installing via Snap did not work well for me. I was able to successfully install it using the installer script. Here's the insturction for [installing using the installer script](https://helm.sh/docs/using_helm/#from-script).
1. Run Kubernetes.<br/>
   Running Kubernetes depends on how you set up your Kubernetes cluster.
1. Install Cassandra Helm Chart.
   I provided a simple, quick-and-dirty script for installing the Helm Chart: [`deploy-cassandra.sh`](deploy-cassandra.sh). Note that some configurations are hardcoded in `deploy` function. I set it to build a cluster with 2 nodes (`config.cluster_size=2`), 1G of max heap space (`config.max_heap_size=1024M`), and 1Gi of persistence storage (`persistence.size=1Gi`).<br/>
   To deploy the cluster:
   ```sh
   sh deploy-cassandra.sh --deploy
   ```
   To tear down the cluster:
   ```sh
   sh deploy-cassandra.sh --destroy
   ```
   To print help:
   ```sh
   sh deploy-cassandra.sh --help
   ```
   1. Build the Cassandra cluster.<br/>
      The command below should download the Helm Chart and install the chart with the default name "cassandra" and in the namespace "cassandra". Note that the name and namespace can be overridden by specifying `-n|--name` and `-ns|--namespace` respectively.
      ```sh
      sh deploy-cassandra.sh --deploy
      ```
      It can take a few minutes for Kubernetes to build the cluster. You can monitor the status by following the logs from commandline or from Kubernetes dashboard.
   1. Check the status of Cassandra nodes.
      ```sh
      kubectl exec -ti --namespace=cassandra cassandra-0 nodetool status
      ```
      Make sure that both nodes are up (U) and normal (N).
      ```
      Datacenter: datacenter1
      =======================
      Status=Up/Down
      |/ State=Normal/Leaving/Joining/Moving
      --  Address     Load        Tokens       Owns (effective)  Host ID                               Rack
      UN  10.1.0.119  248.66 KiB  256          100.0%            e80b8278-354b-496f-b731-045ac0c1aa39  rack1
      UN  10.1.0.118  239.33 KiB  256          100.0%            4fc1ca32-4be5-4ecd-bbc9-6bbcbbf92f09  rack1
      ```
      If you see something like this:
      ```
      Datacenter: datacenter1
      =======================
      Status=Up/Down
      |/ State=Normal/Leaving/Joining/Moving
      --  Address     Load        Tokens  Owns (effective)  Host ID                               Rack
      UN  10.1.0.109  165.51 KiB  256     49.4%             25e8a403-53cd-40e1-89ab-fe841e03294e  rack1
      DN  10.1.0.92   ?           256     50.6%             94caf9f1-5c0d-4ea6-92ea-a4c6a3588322  rack1
      ```
      try re-deploying the cluster by destorying (`--destroy`) and deploying again.
   1. Open CQL shell and run a few queries.
      ```sh
      kubectl exec -ti --namespace=cassandra cassandra-0 cqlsh
      ```
      1. Create a keyspace, which is equivalent to a database in SQL.
         ```sql
         CREATE KEYSPACE test_db WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
         ```
      1. Switch to the newly created `test_db` keyspace.
         ```sql
         USE test_db;
         ```
      1. Create a dummy table for testing.
         ```sql
         CREATE TABLE t (
           pk int,
           t int,
           v text,
           s text static,
           PRIMARY KEY (pk, t)
         );
         ```
      1. Populate the table by issuing a few `INSERT INTO` statements.
         ```sql
         INSERT INTO t (pk, t, v) VALUES (1, 0, 'val1');
         INSERT INTO t (pk, t, v) VALUES (2, 0, 'val2');
         INSERT INTO t (pk, t, v) VALUES (3, 0, 'val3');
         INSERT INTO t (pk, t, v) VALUES (3, 1, 'val3');
         INSERT INTO t (pk, t, v) VALUES (3, 2, 'val3');
         ```
      1. View the contents of the table.
         ```sql
         SELECT * FROM t;
         ```
      1. Quit the shell.
         ```
         quit;
         ```  
1. Build Spark image.
   ```sh
   sh build-spark-docker.sh -t spark-docker build
   ```
   Note "spark-docker" is an arbitrary tag (`-t`) name. You can change it to whatever you want. The command downloads Spark 2.4.0 release (unless you changed `SPARK_VERSION` to another version), compiles [`spark-cassandra`](spark-cassandra) SBT proejct, and builds Docker image with them.
1. Submit Spark application.<br/>
   Make sure that `spark-2.4.0-bin-hadoop2.7` directory, which should have been created by the previous step, exists before running the commands below.
   1. Read the Cassandra table `t` in keyspace `test_db` via Spark.
      ```sh
      spark-2.4.0-bin-hadoop2.7/bin/spark-submit  \
        --master k8s://https://localhost:6443  \
        --deploy-mode cluster  \
        --conf spark.executor.instances=1  \
        --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  \
        --conf spark.kubernetes.container.image=spark:spark-docker  \
        --class chrism.spark.cassandra.ConnectCassandra  \
        --name cassandra-test  \
        local:///opt/spark/dependencies/spark-cassandra-assembly-0.0.1.jar \
          --cassandra-host cassandra-0.cassandra.cassandra \
          --keyspace test_db \
          --table t \
          --operation read \
          --num-rows 20
      ```
      * Note 1: If you changed the tag name from "spark-docker" to something else, you need to update the conf `spark.kubernetes.container.image=spark:spark-docker` accordingly.<br/>
      * Note 2: `local:///opt/spark/dependencies/spark-cassandra-assembly-0.0.1.jar` is the location of the jar within the Docker image.<br/>
      * Note 3: The following arguments are the arguments for `chrism.spark.cassandra.ConnectCassandra`.
      ```
      --cassandra-host cassandra-0.cassandra.cassandra \
      --keyspace test_db \
      --table t \
      --operation read \
      --num-rows 20
      ```
      You can learn more about `spark-submit` command [here](https://spark.apache.org/docs/latest/submitting-applications.html#launching-applications-with-spark-submit).<br/>
      Note that similar to YARN, if you run the application in cluster mode (`--deploy-mode cluster`), you need to check the logs of the driver pod.
   1. Create and write to a new Cassandra table via Spark.
      ```sh
      spark-2.4.0-bin-hadoop2.7/bin/spark-submit  \
        --master k8s://https://localhost:6443  \
        --deploy-mode cluster  \
        --conf spark.executor.instances=1  \
        --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  \
        --conf spark.kubernetes.container.image=spark:spark-docker  \
        --class chrism.spark.cassandra.ConnectCassandra  \
        --name cassandra-test  \
        local:///opt/spark/dependencies/spark-cassandra-assembly-0.0.1.jar \
          --cassandra-host cassandra-0.cassandra.cassandra \
          --keyspace test_db \
          --table t_spark \
          --operation write \
          --num-rows 1000
      ```
      Note that the schema of the table being created is defined in `spark-cassandra` proeject. Spark reflectively creates the schema (`StructType`) from the class [`DummySchema`](spark-cassandra/src/main/scala/chrism/spark/cassandra/DummySchema.scala).
   1. Read the table you just created.
      ```sh
      spark-2.4.0-bin-hadoop2.7/bin/spark-submit  \
        --master k8s://https://localhost:6443  \
        --deploy-mode cluster  \
        --conf spark.executor.instances=1  \
        --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  \
        --conf spark.kubernetes.container.image=spark:spark-docker  \
        --class chrism.spark.cassandra.ConnectCassandra  \
        --name cassandra-test  \
        local:///opt/spark/dependencies/spark-cassandra-assembly-0.0.1.jar \
          --cassandra-host cassandra-0.cassandra.cassandra \
          --keyspace test_db \
          --table t_spark \
          --operation read \
          --num-rows 20
      ```
      Again, the rows are being printed to the console of the driver pod.
1. Tear down Cassandra cluster.
   ```sh
   sh deploy-cassandra.sh --destroy
   ```