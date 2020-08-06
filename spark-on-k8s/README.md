<!---
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

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
* Scala Version: 2.12.10 (You can downgrade, but I recommend 2.12.10 or higher)
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
   Note that if you don't specify your own namespace with `--namespace` switch, the script will install the chart in `cassandra` namespace.<br/>
   To create a `namespace` named `cassandra`:
   ```sh
   kubectl create namesapce cassandra
   ```
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
1. Build Spark image.
   ```sh
   sh build-spark-docker.sh -t spark-docker build
   ```
   Note "spark-docker" is an arbitrary tag (`-t`) name. You can change it to whatever you want. The command downloads Spark 2.4.0 release (unless you changed `SPARK_VERSION` to another version), compiles [`spark-cassandra`](spark-cassandra) SBT proejct, and builds Docker image with them.

## Play with Cassandra
Test to make sure that Cassandra cluster is working properly by running some queries.<br/>
Cassandra has its own query language similar to ANSI SQL called Cassandra Query Language (CQL). If you are interested, you can learn more about CQL [here](http://cassandra.apache.org/doc/latest/cql/index.html).

1. Open CQL shell.
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
1. Insert a row.
   ```sql
   INSERT INTO t (pk, t, v, s) VALUES (0, 0, 'val0', 'static0');
   ```
1. View the contents of the table.
   ```sql
   SELECT * FROM t;
   ```
1. Insert another row and view the table again.
   ```sql
   INSERT INTO t (pk, t, v, s) VALUES (0, 1, 'val0', 'static1');
   SELECT * FROM t;
   ```
   Observe what happens to the column `s` and guess what `static` keyword does. If you want to learn more about Cassandra, read [CQL documentation](http://cassandra.apache.org/doc/latest/cql/definitions.html). This is not a Cassandra tutorial :)
1. Populate the table by issuing a few more `INSERT INTO` statements.
   ```sql
   INSERT INTO t (pk, t, v) VALUES (1, 0, 'val1');
   INSERT INTO t (pk, t, v) VALUES (2, 0, 'val2');
   INSERT INTO t (pk, t, v) VALUES (3, 0, 'val3');
   INSERT INTO t (pk, t, v) VALUES (3, 1, 'val3');
   INSERT INTO t (pk, t, v) VALUES (3, 2, 'val3');
   SELECT * FROM t;
   ```
1. Quit the shell.
   ```
   quit;
   ```

## Run Spark against Cassandra
We built a Docker image for Spark so that we can run Spark on Kubernetes. Use the image to submit a Spark application to your Kubernetes cluster.<br/>
Make sure that `spark-3.0.0-bin-hadoop2.7` directory, which should have been created when you created the image using `build-spark-docker.sh` script, exists before running the commands below unless you already set up `spark-submit` command on your environment. If you did, the version must be 3.0.0; otherwise, you can run into some version conflicts.

1. Read the Cassandra table `t` in keyspace `test_db` via Spark.
   ```sh
   spark-3.0.0-bin-hadoop2.7/bin/spark-submit  \
     --master k8s://https://localhost:6443 \
     --deploy-mode cluster \
     --conf spark.executor.instances=1 \
     --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
     --conf spark.kubernetes.namespace=spark \
     --conf spark.kubernetes.container.image=spark:spark-docker \
     --class chrism.spark.cassandra.ConnectCassandra \
     --name cassandra-test \
     local:///opt/spark/dependencies/spark-cassandra.jar \
       --cassandra-host cassandra-0.cassandra.cassandra \
       --keyspace test_db \
       --table t \
       --operation read \
       --num-rows 20
   ```
   * Note 1: The master `k8s://https://localhost:6443` needs to be changed if the host and/or port of your Kubernetes master is different.
   * Note 2: `spark.kubernetes.authenticate.driver.serviceAccountName` is set to `spark`. See Troubleshooting section below for setting service account.
   * Note 3: `spark.kubernetes.namespace` is set to `spark`. If you are using another namespace, change `spark` to the namespace you intend to use. If there is no namespace called `spark` and it does not exist yet, you need to create one:
     ```sh
     kubectl create namespace spark
     ```
   * Note 4: If you changed the tag name from "spark-docker" to something else, you need to update the conf `spark.kubernetes.container.image=spark:spark-docker` accordingly.
   * Note 5: `local:///opt/spark/dependencies/spark-cassandra.jar` is the location of the jar within the Docker image.
   * Note 6: The following arguments are the arguments for `chrism.spark.cassandra.ConnectCassandra`.
     ```
     --cassandra-host cassandra-0.cassandra.cassandra \
     --keyspace test_db \
     --table t \
     --operation read \
     --num-rows 20
     ```
     You can learn more about `spark-submit` command [here](https://spark.apache.org/docs/latest/submitting-applications.html#launching-applications-with-spark-submit).<br/>
     Note that similar to YARN, if you run the application in cluster mode (`--deploy-mode cluster`), you need to check the logs of the driver pod.
   * Note 7: The Cassandra host `cassandra-0.cassandra.cassandra` can only be resolved if you are running both Cassandra and Spark on the same Kubernetes cluster. If you have a Cassandra cluster on a different Kubernetes cluster or if there exists a non-Kubernetized Cassandra cluster you'd like to use, you need to use its external host name.
1. Create and write to a new Cassandra table via Spark.
   ```sh
   spark-3.0.0-bin-hadoop2.7/bin/spark-submit \
      --master k8s://https://localhost:6443 \
      --deploy-mode cluster \
      --conf spark.executor.instances=1 \
      --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
      --conf spark.kubernetes.namespace=spark \
      --conf spark.kubernetes.container.image=spark:spark-docker \
      --class chrism.spark.cassandra.ConnectCassandra \
      --name cassandra-test \
      local:///opt/spark/dependencies/spark-cassandra.jar \
         --cassandra-host cassandra-0.cassandra.cassandra \
         --keyspace test_db \
         --table t_spark \
         --operation write \
         --num-rows 1000
   ```
   Note that the schema of the table being created is defined in `spark-cassandra` proeject. Spark reflectively creates the schema (`StructType`) from the class [`DummySchema`](spark-cassandra/src/main/scala/chrism/spark/cassandra/DummySchema.scala).
1. Read the table you just created.
   ```sh
   spark-3.0.0-bin-hadoop2.7/bin/spark-submit \
      --master k8s://https://localhost:6443 \
      --deploy-mode cluster \
      --conf spark.executor.instances=1 \
      --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
      --conf spark.kubernetes.namespace=spark \
      --conf spark.kubernetes.container.image=spark:spark-docker \
      --class chrism.spark.cassandra.ConnectCassandra  \
      --name cassandra-test \
      local:///opt/spark/dependencies/spark-cassandra.jar \
         --cassandra-host cassandra-0.cassandra.cassandra \
         --keyspace test_db \
         --table t_spark \
         --operation read \
         --num-rows 20
   ```
   Again, the rows are being printed to the console of the driver pod.

While the job is running you can track the logs of the driver pod.<br/>
To view the running pods:
```sh
kubectl --namespace=spark get pods
```
Grab the name of the pod and follow the log:
```sh
kubectl --namespace=spark logs -f cassandra-test-************-driver
```
The name of the pod should be in `cassandra-test-************-driver` format where `************` is the hash generated for the pod by k8s.<br/>
Or if the job already finished, run the command without `-f` switch. Note that the log can be long. I find it easier to read and search via `less`. To do so, you just need to pipe (`|`) the log to `less`:
```sh
kubectl --namespace=spark logs cassandra-test-************-driver | less
```
For the 1st and 3rd read examples, you should see the table contents being printed to the console:
```
+---+-----+-----------------------+----------+--------------------+-----------+--------------------+-------+
|i32|b    |ts                     |d         |d64                 |f32        |l64                 |s      |
+---+-----+-----------------------+----------+--------------------+-----------+--------------------+-------+
|198|false|2020-05-01 16:07:36.517|2020-02-27|0.7194913984903576  |0.18003547 |5395131880680691446 |row_198|
|13 |false|2020-02-24 16:07:36.498|2020-08-02|0.4041761057310618  |0.09477079 |-1318339116158091431|row_13 |
|736|true |2020-03-03 16:07:36.545|2020-08-04|0.7868378768865254  |0.6538195  |4167641218588317774 |row_736|
|804|true |2020-04-12 16:07:36.547|2020-03-29|0.975096485428588   |0.60459566 |7859564989150638900 |row_804|
|132|false|2020-08-02 16:07:36.513|2020-06-22|0.21757774581888278 |0.10132587 |-5204242357778857925|row_132|
|793|false|2020-03-23 16:07:36.547|2020-06-15|0.9785336091398982  |0.7569967  |-4793429771539071051|row_793|
|904|false|2020-02-24 16:07:36.549|2020-03-04|0.10116308411733144 |0.016069472|-7920411263809638126|row_904|
|385|false|2020-02-22 16:07:36.529|2020-07-26|0.7455582323604987  |0.8456487  |-3616077972489277452|row_385|
|127|true |2020-05-05 16:07:36.512|2020-04-11|0.14467451421074673 |0.5106845  |4365175640639947920 |row_127|
|524|true |2020-06-29 16:07:36.537|2020-01-31|0.3990784812989304  |0.26894075 |-202145400765698955 |row_524|
|187|false|2020-07-04 16:07:36.517|2020-03-20|0.7056573671371533  |0.84315085 |-3866751560114123333|row_187|
|705|false|2020-02-04 16:07:36.544|2020-04-27|0.9415046854707679  |0.8810768  |5040370631630953892 |row_705|
|496|true |2020-02-24 16:07:36.535|2020-05-16|0.35700447281608483 |0.5118339  |4905967623046696516 |row_496|
|432|true |2020-04-08 16:07:36.532|2020-08-06|0.051114523696313974|0.3411824  |-6172314940814644531|row_432|
|136|false|2020-01-20 16:07:36.513|2020-04-25|0.41132538908007366 |0.25450236 |6120311480746570197 |row_136|
|196|false|2020-02-12 16:07:36.517|2020-07-28|0.08457580853123448 |0.4823795  |921693061437324973  |row_196|
|901|true |2020-01-28 16:07:36.549|2020-07-24|0.15723383645790046 |0.41341197 |-3555463325484163665|row_901|
|460|false|2020-08-05 16:07:36.533|2020-07-23|0.3707411074537702  |0.48615134 |3561507885825251636 |row_460|
|694|false|2020-06-29 16:07:36.543|2020-07-29|0.5085788228942967  |0.572513   |1495792723247370959 |row_694|
|59 |false|2020-06-23 16:07:36.506|2020-06-06|0.633546258933422   |0.14018756 |-1412030735363324971|row_59 |
+---+-----+-----------------------+----------+--------------------+-----------+--------------------+-------+
```

## Tear down Cassandra
When you are done, you can tear down the Cassandra cluster with the following command:
```sh
sh deploy-cassandra.sh --destroy
```

## Troubleshooting

### Kubernetes RBAC
When you submit a Spark job to Kubernetes and the job fails due to the following message:

>Exception in thread "main" io.fabric8.kubernetes.client.KubernetesClientException: Failure executing: POST at: https://\<k8s-apiserver-host\>:\<k8s-apiserver-port\>/api/v1/namespaces/default/pods. Message: Forbidden! User kubernetes-admin doesn't have permission. pods "cassandra-test-1555362665973-driver" is forbidden: error looking up service account default/spark: serviceaccount "spark" not found.

Chances are that RBAC is enabled in your Kubernetes cluster. You can read more about RBAC [here](https://spark.apache.org/docs/latest/running-on-kubernetes.html#rbac). To resolve this issue, you need to create a service account named `spark` and grant `edit` `ClusterRole` to `spark`. Note `--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark` in `spark-submit` commands above. We are submitting the job as `spark` and therefore `spark` needs to have necessary permissions.<br/>
Check if the service account `spark` exists:
```sh
kubectl get serviceaccounts
```
If `spark` is listed, the service account exists. If not, run
```sh
kubectl create serviceaccount spark
```
to create the service account. Or if you are using another namespace, use `--namespace` switch to specify the namespace. For instance, if you are running the pods from a namespace called `spark`:
```sh
kubectl --namespace=spark create serviceaccount spark
```
After creating the service account `spark`, grant `edit` `ClusterRole` to `spark`:
```sh
kubectl create clusterrolebinding spark-role \
  --clusterrole=edit \
  --serviceaccount=default:spark \
  --namespace=default
```
Note that this command grants the role in `default` namespace. If you are submitting your Spark jobs to another namespace, you need to use that namespace.
Example:
```sh
kubectl create clusterrolebinding spark-role-ns-spark \
  --clusterrole=edit \
  --serviceaccount=spark:spark \
  --namespace=spark
```
