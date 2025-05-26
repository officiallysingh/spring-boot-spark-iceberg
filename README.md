# Spring Boot Spark Demo using Iceberg Data Lakehouse

## Requirements
This application requires:

- [Java 17](https://sdkman.io/install/)
- [Scala 2.13.16](https://sdkman.io/install/)
- [spark-3.5.5-bin-hadoop3](https://www.apache.org/dyn/closer.lua/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz)
- [Apache Hadoop](https://hadoop.apache.org/releases.html)
- [Apache Hive 4.0.1](https://dlcdn.apache.org/hive/hive-4.0.1/apache-hive-4.0.1-bin.tar.gz)
- `maven`
- `docker`

Recommended [sdkman](https://sdkman.io/install/) for managing Java, Scala and even Spark installations.

**Refer to** [**Apache Hadoop and Hive installation guide**](https://medium.com/@officiallysingh/install-apache-hadoop-and-hive-on-mac-m3-7933e509da90) **for details on how to install Hadoop and Hive**.

## IntelliJ Run Configurations
* Got to main class [**SparkIcebergApplication**](src/main/java/com/ksoot/spark/iceberg/SparkIcebergApplication.java) and Modify run
  configurations as follows.
* Go to `Modify options`, click on `Add VM options` and set the value as `--add-exports java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED`  
  to avoid exception `Factory method 'sparkSession' threw exception with message: class org.apache.spark.storage.StorageUtils$ (in unnamed module @0x2049a9c1) cannot access class sun.nio.ch.DirectBuffer (in module java.base) because module java.base does not export sun.nio.ch to unnamed module @0x2049a9c1`  
  and to Run this Spring boot application in active profile `local`.
* Go to `Modify options` and set active profile as either `local` or `docker` by setting either `-Dspring.profiles.active=local` or  `-Dspring.profiles.active=docker`.
* Go to `Modify options` and make sure `Add dependencies with "provided" scope to classpath` is checked.
* [Configure Formatter in intelliJ](https://github.com/google/google-java-format/blob/master/README.md#intellij-android-studio-and-other-jetbrains-ides), refer to [fmt-maven-plugin](https://github.com/spotify/fmt-maven-plugin) for details.
* Make sure environment variable `SPARK_HOME` is set to local spark installation, find at [spark-3.5.5-bin-hadoop3](https://dlcdn.apache.org/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz).
* Make sure environment variable `M2_REPO` is set to local maven repository i.e. `<your user home>/.m2/repository`

Run [**SparkIcebergApplication**](src/main/java/com/ksoot/spark/iceberg/SparkIcebergApplication.java) as Spring boot application.


> [!IMPORTANT]
> Its temporary project would be deleted later.
