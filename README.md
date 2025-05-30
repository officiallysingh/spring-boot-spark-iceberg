# Spring Boot Spark integration with Apache Iceberg Data Lakehouse
Apache Iceberg is a high-performance table format for huge analytic datasets, enabling ACID operations (create, update, delete) and supporting time travel, schema evolution, and efficient data ingestion.  
Integrating Iceberg with Spark and Spring Boot allows you to build robust data lake applications with modern engineering practices.

**Key Features of [Apache Iceberg](https://iceberg.apache.org)**:
- ACID transactions on data lakes
- Versioned table snapshots for time travel and rollback
- Schema evolution without rewriting data
- Support for multiple catalog types: Hadoop, Hive, and Nessie

## Prerequisites
This project requires:

- [Java 17](https://sdkman.io/install/)
- [Scala 2.13.16](https://sdkman.io/install/)
- [Spark 3.5.5](https://www.apache.org/dyn/closer.lua/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz)
- [Apache Hadoop](https://hadoop.apache.org/releases.html)
- [Apache Hive 4.0.1](https://dlcdn.apache.org/hive/hive-4.0.1/apache-hive-4.0.1-bin.tar.gz)
- [Docker](https://www.docker.com), Make sure Docker is allocated with enough resources.
- [Maven](https://maven.apache.org), Make sure environment variable `M2_REPO` is set to local maven repository
- IDE (IntelliJ, Eclipse or VS Code), Recommended [IntelliJ IDEA](https://www.jetbrains.com/idea).
- Optional [Configure Formatter in intelliJ](https://github.com/google/google-java-format/blob/master/README.md#intellij-android-studio-and-other-jetbrains-ides), refer to [fmt-maven-plugin](https://github.com/spotify/fmt-maven-plugin) for details.

Recommended [sdkman](https://sdkman.io/install/) for managing Java, Scala and even Spark installations.

## Features
- **Spring Boot**: REST API layer and dependency management.
- **Spark**: Data processing engine, handles distributed data processing.
- **Iceberg**: Manages the table format and metadata.
- **Catalogs**: Table management.
- **Storage**: HDFS/Local FS/AWS S3/GCS/Azure Blob Storage.

> [!IMPORTANT]  
> **Refer to** [**Spark Spring Boot starter**](https://medium.com/@officiallysingh/spark-spring-boot-starter-e206def765b9) **for details on how Spark and Iceberg beans are auto-configured using `application.yml` configurations**.  
> **Refer to** [**Apache Hadoop and Hive installation guide**](https://medium.com/@officiallysingh/install-apache-hadoop-and-hive-on-mac-m3-7933e509da90) **for details on how to install Hadoop and Hive**.

## Iceberg
Iceberg can be configured to use different catalog types and storage backends.  
**This demo has been tested to use following three catalog types**:
* [**`HadoopCatalog`**](https://iceberg.apache.org/docs/1.9.0/java-api-quickstart/#using-a-hadoop-catalog).
* [**`HiveCatalog`**](https://iceberg.apache.org/docs/1.9.0/java-api-quickstart/#using-a-hive-catalog).
* [**`NessieCatalog`**](https://iceberg.apache.org/docs/1.9.0/nessie/).

**And following two storage backends**:
* Local Hadoop.
* AWS S3.

## Catalog configurations
Following properties can be passed as VM options while running the application to choose Catalog type and Storage type.  
For details refer to [**`application.yml`**](src/main/resources/config/application.yml) file.
* `**CATALOG_TYPE**`: Type of catalog to use, can be `hadoop`, `hive` or `nessie`. e.g. `-DCATALOG_TYPE=hadoop`.
* `**STORAGE_TYPE**`: Type of storage to use, can be `hadoop` or `aws-s3`. e.g. `-DSTORAGE_TYPE=hadoop`.

> [!IMPORTANT]
> [Other Catalog types](https://iceberg.apache.org/docs/latest/spark-configuration/#catalog-configuration), Azure Blob Storage and Google Cloud Storage (GCS) are also supported as data storage for Iceberg tables.  
> You need to have required dependencies in your classpath and configure catalog properties accordingly in `application.yml` or `application.properties` file.

### Setting up AWS S3 for Data storage
- Configure [AWS Cli](https://aws.amazon.com/cli/) with your AWS credentials and region.
- Add following dependencies to your `pom.xml`:
```xml
<properties>
    <spring-cloud-aws.version>3.3.1</spring-cloud-aws.version>
    <aws-java-sdk.version>2.28.28</aws-java-sdk.version>
</properties>

<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.awspring.cloud</groupId>
            <artifactId>spring-cloud-aws-dependencies</artifactId>
            <version>${spring-cloud-aws.version}</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
        <dependency>
            <groupId>software.amazon.awssdk</groupId>
            <artifactId>bom</artifactId>
            <version>${aws-java-sdk.version}</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <dependency>
        <groupId>io.awspring.cloud</groupId>
        <artifactId>spring-cloud-aws-starter-s3</artifactId>
    </dependency>
    <dependency>
        <groupId>software.amazon.awssdk</groupId>
        <artifactId>bundle</artifactId>
        <version>${awssdk-bundle.version}</version>
    </dependency>
</dependencies>
```
- Add the following properties to your `application.yml` or `application.properties` file:
```yaml
spring:
  cloud:
    aws:
      credentials:
        access-key: ${AWS_ACCESS_KEY:<Your AWS Access Key>}
        secret-key: ${AWS_SECRET_KEY:<Your AWS Secret Key>}
      region:
        static: ${AWS_REGION:<Your AWS Region>}
      s3:
        endpoint: ${AWS_S3_ENDPOINT:https://s3.<Your AWS Region>.amazonaws.com}
```
- Update **$HIVE_HOME/conf/hive-site.xml** with following properties.
```xml
    <property>
        <name>fs.s3a.access.key</name>
        <value>Your AWS Access Key</value>
    </property>
    <property>
        <name>fs.s3a.secret.key</name>
        <value>Your AWS Secret Key</value>
    </property>
    <property>
        <name>fs.s3a.endpoint</name>
        <value>s3.{Your AWS Region}.amazonaws.com</value>
    </property>
    <property>
        <name>fs.s3a.impl</name>
        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
    </property>
```
- Add `aws-java-sdk-bundle-1.12.262.jar`, `hadoop-aws-3.3.4.jar` and `postgresql-42.7.4.jar` to folder `$HIVE_HOME/lib`. Versions may vary, so make sure to use compatible versions with your setup.
- **Spark Hadoop Configurations**
  Each catalog stores its metadata in its own storage such as Postgres (or any other relational database) for Hive, MongoDB for Nessie etc.
  But the table's data is stored in a distributed file system such as HDFS, S3, Azure Blob Storage or Google Cloud Storage (GCS).  
  **So you need to set Spark Hadoop configurations, either you can configure them globally as follows, which will be used by all Catalogs configured in your application.**
```yaml
spark:
  hadoop:
    fs:
      s3a:
        aws.credentials.provider: "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        access.key: ${AWS_ACCESS_KEY:<Your AWS Access Key>}
        secret.key: ${AWS_SECRET_KEY:<Your AWS Secret Key>}
        endpoint: ${AWS_S3_ENDPOINT:s3.<Your AWS Region>.amazonaws.com}
        impl: org.apache.hadoop.fs.s3a.S3AFileSystem
        path.style.access: true  # For path-style access, useful in some S3-compatible services
        connection.ssl.enabled: false  # Enable SSL
        fast.upload: true  # Enable faster uploads
```

### Iceberg Catalog Configuration with Local Hadoop as Data storage
**For Spark Iceberg integration demo refer to** [**`spring-boot-spark-iceberg`**](https://github.com/officiallysingh/spring-boot-spark-iceberg).

Following are Iceberg Catalog configurations using Local Hadoop as Data storage.
#### Hadoop Catalog
Configure Hadoop Catalog as follows. Catalog name is also set to `hadoop` but it can be any name you want.
```yaml
spark:
  sql:
    catalog:
      hadoop: org.apache.iceberg.spark.SparkCatalog
      hadoop.type: hadoop
      hadoop.warehouse: ${CATALOG_WAREHOUSE:hdfs://localhost:9000/warehouse}
      hadoop.uri: ${CATALOG_URI:hdfs://localhost:9000}
      hadoop.default-namespace: ${CATALOG_NAMESPACE:ksoot}
      hadoop.io-impl: org.apache.iceberg.hadoop.HadoopFileIO
```

#### Hive Catalog
Configure Hive Catalog as follows. Catalog name is also set to `hive` but it can be any name you want.
```yaml
spark:
  sql:
    catalog:
      hadoop: org.apache.iceberg.spark.SparkCatalog
      hadoop.type: hadoop
      hadoop.warehouse: ${CATALOG_WAREHOUSE:hdfs://localhost:9000/warehouse}
      hadoop.uri: ${CATALOG_URI:hdfs://localhost:9000}
      hadoop.default-namespace: ${CATALOG_NAMESPACE:ksoot}
      hadoop.io-impl: org.apache.iceberg.hadoop.HadoopFileIO
```

#### Nessie Catalog
Configure Nessie Catalog as follows. Catalog name is also set to `nessie` but it can be any name you want.
```yaml
spark:
  sql:
    catalog:
      nessie: org.apache.iceberg.spark.SparkCatalog
      nessie.type: nessie
      nessie.warehouse: ${CATALOG_WAREHOUSE:hdfs://localhost:9000/warehouse}
      nessie.uri: ${CATALOG_URI:http://localhost:19120/api/v2}
      nessie.default-namespace: ${CATALOG_NAMESPACE:ksoot}
      nessie.io-impl: org.apache.iceberg.hadoop.HadoopFileIO
```

### Iceberg Catalog Configuration with AWS S3 as Data storage
Along with the catalog configurations, you also need to do following.
- Configure [AWS Cli](https://aws.amazon.com/cli/) with your AWS credentials and region.
- Add following dependencies to your `pom.xml`:
```xml
<properties>
    <spring-cloud-aws.version>3.2.1</spring-cloud-aws.version>
    <awssdk-bundle.version>2.25.70</awssdk-bundle.version>
</properties>

<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.awspring.cloud</groupId>
            <artifactId>spring-cloud-aws-dependencies</artifactId>
            <version>${spring-cloud-aws.version}</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <dependency>
        <groupId>io.awspring.cloud</groupId>
        <artifactId>spring-cloud-aws-starter-s3</artifactId>
    </dependency>
    <dependency>
        <groupId>software.amazon.awssdk</groupId>
        <artifactId>bundle</artifactId>
        <version>${awssdk-bundle.version}</version>
    </dependency>
</dependencies>
```
- Add the following properties to your `application.yml` or `application.properties` file:
```yaml
spring:
  cloud:
    aws:
      credentials:
        access-key: ${AWS_ACCESS_KEY:<Your AWS Access Key>}
        secret-key: ${AWS_SECRET_KEY:<Your AWS Secret Key>}
      region:
        static: ${AWS_REGION:<Your AWS Region>}
      s3:
        endpoint: ${AWS_S3_ENDPOINT:https://s3.<Your AWS Region>.amazonaws.com}
```
- Update **$HIVE_HOME/conf/hive-site.xml** with following properties.
```xml
    <property>
        <name>fs.s3a.access.key</name>
        <value>Your AWS Access Key</value>
    </property>
    <property>
        <name>fs.s3a.secret.key</name>
        <value>Your AWS Secret Key</value>
    </property>
    <property>
        <name>fs.s3a.endpoint</name>
        <value>s3.{Your AWS Region}.amazonaws.com</value>
    </property>
    <property>
        <name>fs.s3a.impl</name>
        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
    </property>
```
- Add `aws-java-sdk-bundle-1.12.262.jar`, `hadoop-aws-3.3.4.jar` and `postgresql-42.7.4.jar` to folder `$HIVE_HOME/lib`. Versions may vary, so make sure to use compatible versions with your setup.

- Spark Hadoop Configurations
Each catalog stores its metadata in its own storage such as Postgres (or any other relational database) for Hive, MongoDB for Nessie etc.
But the table's data is stored in a distributed file system such as HDFS, S3, Azure Blob Storage or Google Cloud Storage (GCS).  
**So you need to set Spark Hadoop configurations, either you can configure them globally as follows, which will be used by all Catalogs configured in your application.**

```yaml
spark:
  hadoop:
    fs:
      s3a:
        aws.credentials.provider: "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        access.key: ${AWS_ACCESS_KEY:<Your AWS Access Key>}
        secret.key: ${AWS_SECRET_KEY:<Your AWS Secret Key>}
        endpoint: ${AWS_S3_ENDPOINT:s3.<Your AWS Region>.amazonaws.com}
        impl: org.apache.hadoop.fs.s3a.S3AFileSystem
        path.style.access: true  # For path-style access, useful in some S3-compatible services
        connection.ssl.enabled: false  # Enable SSL
        fast.upload: true  # Enable faster uploads
```

**Or you can configure them in each catalog configuration as shown in the following sections, if it's different from the global configurations.**

**Following are Iceberg Catalog configurations using AWS S3 as Data storage.**

#### Hadoop Catalog with AWS S3
Configure Hadoop Catalog as follows. Catalog name is also set to `hadoop` but it can be any name you want.
```yaml
spark:
  sql:
    catalog:
      hadoop: org.apache.iceberg.spark.SparkCatalog
      hadoop.type: hadoop
      hadoop.warehouse: ${CATALOG_WAREHOUSE:s3a://<Your S3 Bucket Name>/warehouse}
      hadoop.uri: ${CATALOG_URI:hdfs://localhost:9000}
      hadoop.default-namespace: ${CATALOG_NAMESPACE:ksoot}
      hadoop.io-impl: org.apache.iceberg.aws.s3.S3FileIO
      hadoop.hadoop.fs.s3a.access.key: ${AWS_ACCESS_KEY:<Your AWS Access Key>}
      hadoop.hadoop.fs.s3a.secret.key: ${AWS_SECRET_KEY:<Your AWS Secret Key>}
      hadoop.hadoop.fs.s3a.endpoint: ${AWS_S3_ENDPOINT:s3.<Your AWS Region>.amazonaws.com}
      hadoop.hadoop.fs.s3a.impl: org.apache.hadoop.fs.s3a.S3AFileSystem
      hadoop.hadoop.fs.s3a.path.style.access: true  # For path-style access, useful in some S3-compatible services
      hadoop.hadoop.fs.s3a.connection.ssl.enabled: false  # Enable SSL
      hadoop.hadoop.fs.s3a.fast.upload: true  # Enable faster uploads
```

#### Hive Catalog with AWS S3
Configure Hive Catalog as follows. Catalog name is also set to `hive` but it can be any name you want.
```yaml
spark:
  sql:
    catalog:
      hive: org.apache.iceberg.spark.SparkCatalog
      hive.type: hive
      hive.warehouse: ${CATALOG_WAREHOUSE:s3a://<Your S3 Bucket Name>/warehouse}
      hive.uri: ${CATALOG_URI:thrift://localhost:9083}
      hive.default-namespace: ${CATALOG_NAMESPACE:ksoot}
      hive.io-impl: org.apache.iceberg.aws.s3.S3FileIO
      hive.hadoop.fs.s3a.access.key: ${AWS_ACCESS_KEY:<Your AWS Access Key>}
      hive.hadoop.fs.s3a.secret.key: ${AWS_SECRET_KEY:<Your AWS Secret Key>}
      hive.hadoop.fs.s3a.endpoint: ${AWS_S3_ENDPOINT:s3.<Your AWS Region>.amazonaws.com}
      hive.hadoop.fs.s3a.impl: org.apache.hadoop.fs.s3a.S3AFileSystem
      hive.hadoop.fs.s3a.path.style.access: true  # For path-style access, useful in some S3-compatible services
      hive.hadoop.fs.s3a.connection.ssl.enabled: false  # Enable SSL
      hive.hadoop.fs.s3a.fast.upload: true  # Enable faster uploads
```

> [!IMPORTANT]
> Add `aws-java-sdk-bundle-1.12.262.jar`, `hadoop-aws-3.3.4.jar` and `postgresql-42.7.4.jar` to folder `$HIVE_HOME/lib`.
> Versions may vary, so make sure to use compatible versions with your setup.

#### Nessie Catalog with AWS S3
Configure Nessie Catalog as follows. Catalog name is also set to `nessie` but it can be any name you want.
```yaml
spark:
  sql:
    catalog:
      nessie: org.apache.iceberg.spark.SparkCatalog
      nessie.type: nessie
      nessie.warehouse: ${CATALOG_WAREHOUSE:s3a://<Your S3 Bucket Name>/warehouse}
      nessie.uri: ${CATALOG_URI:http://localhost:19120/api/v2}
      nessie.default-namespace: ${CATALOG_NAMESPACE:ksoot}
      nessie.io-impl: org.apache.iceberg.aws.s3.S3FileIO
      nessie.hadoop.fs.s3a.access.key: ${AWS_ACCESS_KEY:<Your AWS Access Key>}
      nessie.hadoop.fs.s3a.secret.key: ${AWS_SECRET_KEY:<Your AWS Secret Key>}
      nessie.hadoop.fs.s3a.endpoint: ${AWS_S3_ENDPOINT:s3.<Your AWS Region>.amazonaws.com}
      nessie.hadoop.fs.s3a.impl: org.apache.hadoop.fs.s3a.S3AFileSystem
      nessie.hadoop.fs.s3a.path.style.access: true  # For path-style access, useful in some S3-compatible services
      nessie.hadoop.fs.s3a.connection.ssl.enabled: false  # Enable SSL
      nessie.hadoop.fs.s3a.fast.upload: true  # Enable faster uploads
```

> [!IMPORTANT]
> You can also configure multiple catalogs in your Spark application, for example, you can have both Hadoop and Hive catalogs configured in your application, 
> but choose the one you want to use in your Spark pipelines at runtime.  
> You can see we need to set AWS S3 and Hadoop S3A configurations at multiple places because of following reasons
> - Spring boot application is not picking up the region from application.yml configurations, so need to set it through AWS Cli settings.
> - Apache Hive accesses AWS S3 through Hadoop S3A configurations, so need to set them in `hive-site.xml`.
> - AWS_ACCESS_KEY, AWS_SECRET_KEY and AWS_S3_ENDPOINT need to be set in `application.yml` to make them available to Spark at runtime.



#### Manual
All these services can be installed locally on your machine, and should be accessible at above-mentioned urls and credentials (wherever applicable).

#### Docker compose
* The [compose.yml](compose.yml) file defines the services and configurations to run required infrastructure in Docker.
* In Terminal go to project root `spring-boot-spark-kubernetes` and execute following command and confirm if all services are running.
```shell
docker compose up -d
```
* Create databases `spark_jobs_db` and `error_logs_db` in Postgres and Kafka topics `job-stop-requests` and `error-logs` if they do not exist.

> [!IMPORTANT]  
> While using docker compose make sure the required ports are free on your machine, otherwise port busy error could be thrown.

## Spark Configurations


## IntelliJ Run Configurations
* Got to main class [**SparkIcebergApplication**](src/main/java/com/ksoot/spark/iceberg/SparkIcebergApplication.java) and Modify run configurations as follows.
* Go to `Modify options`, click on `Add VM options` and set the value as `--add-exports java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED`  
  to avoid exception `Factory method 'sparkSession' threw exception with message: class org.apache.spark.storage.StorageUtils$ (in unnamed module @0x2049a9c1) cannot access class sun.nio.ch.DirectBuffer (in module java.base) because module java.base does not export sun.nio.ch to unnamed module @0x2049a9c1`  
  and to Run this Spring boot application in active profile `local`.
* Go to `Modify options` and set active profile as either `local` or `docker` by setting either `-Dspring.profiles.active=local` or  `-Dspring.profiles.active=docker` respectively.
* Go to `Modify options` and set Catalog type as either `hadoop`, `hive` or `nessie` by setting either `-DCATALOG_TYPE=hadoop` or `-DCATALOG_TYPE=hive` or `-DCATALOG_TYPE=nessie` respectively.
* Go to `Modify options` and make sure `Add dependencies with "provided" scope to classpath` is checked.
* [Configure Formatter in intelliJ](https://github.com/google/google-java-format/blob/master/README.md#intellij-android-studio-and-other-jetbrains-ides), refer to [fmt-maven-plugin](https://github.com/spotify/fmt-maven-plugin) for details.

Run [**SparkIcebergApplication**](src/main/java/com/ksoot/spark/iceberg/SparkIcebergApplication.java) as Spring boot application.


> [!IMPORTANT]
> Its imp.
