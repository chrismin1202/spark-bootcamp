/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

organization := "chrism.sdsc"
name := "scala-sbt"

version := "0.0.1"

scalaVersion := "2.12.10"

licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0"))

homepage := Some(url("https://github.com/chrismin1202/scala-sbt"))
scmInfo := Some(ScmInfo(url("https://github.com/chrismin1202/scala-sbt"), "git@github.com:chrismin1202/scala-sbt.git"))
developers := List(
  Developer("chrismin1202", "Donatello", "chrism.1202@gmail.com", url("https://github.com/chrismin1202"))
)

parallelExecution in Test := false

fork in Test := true

javaOptions in Test += "-Djdk.logging.allowStackWalkSearch=true"

connectInput in Test := true

val sparkVersion = "3.0.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalacheck" %% "scalacheck" % "1.14.0" % Test,
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.specs2" %% "specs2-core" % "4.7.0" % Test
)

publishMavenStyle := true
publishArtifact in Test := true
updateOptions := updateOptions.value.withGigahorse(false)
pomExtra :=
  <licenses>
    <license>
      <name>Apache 2</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:chrismin1202/scala-sbt.git</url>
    <connection>scm:git:git@github.com:chrismin1202/scala-sbt.git</connection>
  </scm>
  <developers>
    <developer>
      <id>donatello</id>
      <name>Donatello</name>
    </developer>
  </developers>
