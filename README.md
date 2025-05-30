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
- [Scala 2.13.16](https://sdkman.io/install/). Make sure scala version is printed as `2.13.16` in terminal by executing `scala -version`.
- [Spark 3.5.5](https://www.apache.org/dyn/closer.lua/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz)
- [Apache Hadoop](https://hadoop.apache.org/releases.html)
- [Apache Hive 4.0.1](https://dlcdn.apache.org/hive/hive-4.0.1/apache-hive-4.0.1-bin.tar.gz)
- [Docker](https://www.docker.com)
- [Maven](https://maven.apache.org), Make sure environment variable `M2_REPO` is set to local maven repository
- IDE (IntelliJ, Eclipse or VS Code), Recommended [IntelliJ IDEA](https://www.jetbrains.com/idea).
- Optional [Configure Formatter in intelliJ](https://github.com/google/google-java-format/blob/master/README.md#intellij-android-studio-and-other-jetbrains-ides), refer to [fmt-maven-plugin](https://github.com/spotify/fmt-maven-plugin) for details.

**Recommended [sdkman](https://sdkman.io/install/) for managing Java, Scala and even Spark installations.**

## Features
- **Spring Boot**: REST API layer and dependency management.
- **Spark**: Data processing engine, handles distributed data processing.
- **Iceberg**: Manages the table format and metadata.
- **Catalogs**: Table management.
- **Storage**: Local FS/HDFS/AWS S3/GCS/Azure Blob Storage.

> [!IMPORTANT]  
> **Refer to** [**Spark Spring Boot starter**](https://github.com/officiallysingh/spring-boot-starter-spark) **for details on how Spark and Iceberg beans are auto-configured using `application.yml` configurations**.  
> **Refer to** [**Apache Hadoop and Hive installation guide**](https://medium.com/@officiallysingh/install-apache-hadoop-and-hive-on-mac-m3-7933e509da90) **for details on how to install Hadoop and Hive**.

## Iceberg
Iceberg can be configured to use different catalog types and storage backends. **This demo has been tested to use following three catalog types**:
* [**`HadoopCatalog`**](https://iceberg.apache.org/docs/1.9.0/java-api-quickstart/#using-a-hadoop-catalog).
* [**`HiveCatalog`**](https://iceberg.apache.org/docs/1.9.0/java-api-quickstart/#using-a-hive-catalog).
* [**`NessieCatalog`**](https://iceberg.apache.org/docs/1.9.0/nessie/).

**And following two storage backends**:
* Local Hadoop.
* AWS S3.

## Catalog configurations
Following properties can be passed as VM options while running the application to choose Catalog type and Storage type. 
For details refer to [**`application.yml`**](src/main/resources/config/application.yml) file.
* **CATALOG_TYPE**: Type of catalog to use, can be `hadoop`, `hive` or `nessie`. e.g. `-DCATALOG_TYPE=hadoop`.
* **STORAGE_TYPE**: Type of storage to use, can either be `hadoop` (default) or `aws-s3`. e.g. `-DSTORAGE_TYPE=hadoop` or `-DSTORAGE_TYPE=aws-s3`.

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
- Update **$HIVE_HOME/conf/hive-site.xml** with following properties. It will take AWS credetials from AWS CLI configuration.
  Replace `{Your AWS Region}` with your actual AWS region, e.g. `ap-south-1` etc. It's only required if you are using Hive Catalog with AWS S3 as storage.
```xml
<property>
    <name>fs.s3a.aws.credentials.provider</name>
    <value>com.amazonaws.auth.DefaultAWSCredentialsProviderChain</value>
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
  It's only required if you are using Hive Catalog with AWS S3 as storage.
- **Spark Hadoop Configurations**: 
  Each catalog stores some metadata regarding the tables in its own storage such as Postgres (or any other relational database) for Hive, MongoDB for Nessie etc.
  But the catalog metadata json files and table's data is stored in a distributed file system such as HDFS, S3, Azure Blob Storage or Google Cloud Storage (GCS).  
  **So you need to set Spark Hadoop configurations, either globally as follows, which will be used by all Catalogs configured in your application.**
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
**Or you can configure them in each catalog configuration as shown in the following sections, if it's different from the global configurations.  
It will override the global configurations (if specified) for that particular catalog.**

> [!NOTE]
> Replace catalog_name with the catalog name of your choice, for example, `hadoop`, `hive` or `nessie`.

```yaml
spark:
  sql:
    catalog:
      catalog_name.io-impl: org.apache.iceberg.aws.s3.S3FileIO
      catalog_name.hadoop.fs.s3a.access.key: ${AWS_ACCESS_KEY:<Your AWS Access Key>}
      catalog_name.hadoop.fs.s3a.secret.key: ${AWS_SECRET_KEY:<Your AWS Secret Key>}
      catalog_name.hadoop.fs.s3a.endpoint: ${AWS_S3_ENDPOINT:s3.<Your AWS Region>.amazonaws.com}
      catalog_name.hadoop.fs.s3a.impl: org.apache.hadoop.fs.s3a.S3AFileSystem
      catalog_name.hadoop.fs.s3a.path.style.access: true  # For path-style access, useful in some S3-compatible services
      catalog_name.hadoop.fs.s3a.connection.ssl.enabled: false  # Enable SSL
      catalog_name.hadoop.fs.s3a.fast.upload: true  # Enable faster uploads
```

### Hadoop Catalog
Configure Hadoop Catalog as follows. Catalog name is also set to `hadoop` but it can be any name you want.
```yaml
spark:
  sql:
    catalog:
      hadoop: org.apache.iceberg.spark.SparkCatalog
      hadoop.type: hadoop
      hadoop.uri: ${CATALOG_URI:hdfs://localhost:9000}
      hadoop.default-namespace: ${CATALOG_NAMESPACE:ksoot}
      hadoop.io-impl: org.apache.iceberg.hadoop.HadoopFileIO
```

For Storage type Local Hadoop
```yaml
spark:
  sql:
    catalog:
      hadoop.warehouse: ${CATALOG_WAREHOUSE:hdfs://localhost:9000/warehouse}
      hadoop.io-impl: org.apache.iceberg.hadoop.HadoopFileIO
```

For Storage type AWS S3
```yaml
spark:
  sql:
    catalog:
      hadoop.warehouse: ${CATALOG_WAREHOUSE:s3a://<Your S3 Bucket Name>/warehouse}
      hadoop.io-impl: org.apache.iceberg.aws.s3.S3FileIO
```

### Hive Catalog
Configure Hive Catalog as follows. Catalog name is also set to `hive` but it can be any name you want.
```yaml
spark:
  sql:
    catalog:
      hive: org.apache.iceberg.spark.SparkCatalog
      hive.type: hive
      hive.uri: ${CATALOG_URI:thrift://localhost:9083}
      hive.default-namespace: ${CATALOG_NAMESPACE:ksoot}
```

For Storage type Local Hadoop
```yaml
spark:
  sql:
    catalog:
      hive.warehouse: ${CATALOG_WAREHOUSE:/user/hive/warehouse}
      hive.io-impl: org.apache.iceberg.hadoop.HadoopFileIO
```

For Storage type AWS S3
```yaml
spark:
  sql:
    catalog:
      hive.warehouse: ${CATALOG_WAREHOUSE:s3a://<Your S3 Bucket Name>/warehouse}
      hive.io-impl: org.apache.iceberg.aws.s3.S3FileIO
```

### Nessie Catalog
Configure Nessie Catalog as follows. Catalog name is also set to `nessie` but it can be any name you want.
```yaml
spark:
  sql:
    catalog:
      nessie: org.apache.iceberg.spark.SparkCatalog
      nessie.type: nessie
      nessie.uri: ${CATALOG_URI:http://localhost:19120/api/v2}
      nessie.default-namespace: ${CATALOG_NAMESPACE:ksoot}
```

For Storage type Local Hadoop
```yaml
spark:
  sql:
    catalog:
      nessie.warehouse: ${CATALOG_WAREHOUSE:hdfs://localhost:9000/warehouse}
      nessie.io-impl: org.apache.iceberg.hadoop.HadoopFileIO
```

For Storage type AWS S3
```yaml
spark:
  sql:
    catalog:
      nessie.warehouse: ${CATALOG_WAREHOUSE:s3a://<Your S3 Bucket Name>/warehouse}
      nessie.io-impl: org.apache.iceberg.aws.s3.S3FileIO
```

> [!IMPORTANT]
> You can also configure multiple catalogs in your Spark application, for example, you can have both Hadoop and Hive catalogs configured in your application,
> but choose the one you want to use in your Spark pipelines at runtime.  
> You can see we need to set AWS S3 and Hadoop S3A configurations at multiple places because of following reasons
> - Spring boot application is not picking up the region from application.yml configurations, so need to set it through AWS Cli settings.
> - Apache Hive accesses AWS S3 through Hadoop S3A configurations, so need to set them in `hive-site.xml`.
> - AWS_ACCESS_KEY, AWS_SECRET_KEY and AWS_S3_ENDPOINT need to be set in `application.yml` to make them available to Spark at runtime.

## Table Operations
For details on how to create, update and delete Iceberg tables in Iceberg Catalog, refer to the [**IcebergCatalogClient**](src/main/java/com/ksoot/spark/iceberg/service/IcebergCatalogClient.java) class.  
`IcebergCatalogClient` provides the following methods:
- `tableExists`: Checks if a table exists or not.
- `loadTable`: Get an instance of an Iceberg [table](https://iceberg.apache.org/javadoc/1.9.0/org/apache/iceberg/Table.html).
- `createOrUpdateTable`: Creates an Iceberg table if it does not exist otherwise check if there is schema change as per passed column details, if yes update the table otherwise do nothing.
- `createTable`: Creates an Iceberg table if it does not exist otherwise throws `AlreadyExistsException`.
- `listTables`: Gets a list of all the Iceberg [tables](https://iceberg.apache.org/javadoc/1.9.0/org/apache/iceberg/Table.html) in a configured namespace.
- `dropTable`: Deletes an Iceberg table.
- `renameTable`: Renames an Iceberg table.

## Data Operations
Refer to [SparkIcebergService](src/main/java/com/ksoot/spark/iceberg/service/SparkIcebergService.java) for details on how to write and read data into Iceberg tables.

### Write Data
Following are different variants to write data into Iceberg tables:
- `dataset.writeTo(icebergTable).append()`: Appends the Spark Dataset into Iceberg table. The Table must exist before this operation.
  If the output table does not exist, this operation will fail with `CannotAppendMissingTableException`.
- `dataset.writeTo(icebergTable).replace()`: Replace an existing table with the contents of the data frame.
  The existing table's schema, partition layout, properties, and other configuration will be replaced with the contents of the data frame and the configuration set on this writer.
  If the output table does not exist, this operation will fail with `CannotReplaceMissingTableException`.
- `dataset.writeTo(icebergTable).create()`: Create a new table from the contents of the data frame.
  The new table's schema, partition layout, properties, and other configuration will be based on the configuration set on this writer.
  If the output table exists, this operation will fail with `TableAlreadyExistsException`.
- `dataset.writeTo(icebergTable).createOrReplace()`: Create a new table or replace an existing table with the contents of the data frame.
  The output table's schema, partition layout, properties, and other configuration will be based on the contents of the data frame and the configuration set on this writer. If the table exists, its configuration and data will be replaced.

### Read Data
```java
Dataset<Row> dataset =
    this.sparkSession.read().format("iceberg").table(tableName);
```

> [!IMPORTANT]
> While performing operations on Iceberg tables, the given table name is prefixed with namespace.  
> For example, if the table name is `my_table` and the namespace is `ksoot`, then the effective table name will be `ksoot.my_table`.  
> This is internally handled in classes `IcebergCatalogClient` and `SparkIcebergService`, so in methods exposed by these services, you can just pass the table name without a namespace prefix.

## Running the Application
Run [**SparkIcebergApplication**](src/main/java/com/ksoot/spark/iceberg/SparkIcebergApplication.java) as Spring boot application. Some configurations need to be set as VM options while running the application.  
Also, it may need some services to be running in before starting the application depending on Catalog type and Storage type.  
Application is bundled with [Docker Compose Support](https://www.baeldung.com/docker-compose-support-spring-boot) which is enabled when running the application in `docker` profile by setting VM option `-Dspring.profiles.active=docker`.
Following two VM options are required irrespective of Catalog type and Storage type.
* Go to `Modify options`, click on `Add VM options` and set the value as `--add-exports java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED`  
  to avoid exception `Factory method 'sparkSession' threw exception with message: class org.apache.spark.storage.StorageUtils$ (in unnamed module @0x2049a9c1) cannot access class sun.nio.ch.DirectBuffer (in module java.base) because module java.base does not export sun.nio.ch to unnamed module @0x2049a9c1`
* Go to `Modify options` and set active profile as either `local` or `docker` by setting either `-Dspring.profiles.active=local` or  `-Dspring.profiles.active=docker` respectively.

### Docker compose
The Docker [compose.yml](compose.yml) file defines following services.
* **postgres**: Required for `hive` catalog to use Postgres as its internal database. Comment out if Postgres is installed locally.
* **mongo**: Required for `nessie` catalog to use MongoDB as its internal database. Comment out if MongoDB is installed locally.
* **nessie**: Required for `nessie` catalog.
* **dremio**: Optional, it can be used to browse and query the Iceberg tables.

In Terminal go to project root and execute following command and confirm if required services are running.
```shell
% docker compose up -d
```
To stop the services and delete volumes, execute:
```shell
% docker compose down -v
```
> [!IMPORTANT]  
> While using docker compose make sure the required ports are free on your machine, otherwise port busy error could be thrown.

### With Local Hadoop as Storage
* Make sure Hadoop is installed and running on your machine as per [Hadoop Installation Guide](https://medium.com/@officiallysingh/install-apache-hadoop-and-hive-on-mac-m3-7933e509da90).
* No need to explicitly set Storage type as `hadoop` as it is the default storage type.
* Got to main class [**SparkIcebergApplication**](src/main/java/com/ksoot/spark/iceberg/SparkIcebergApplication.java) and Modify run configurations as follows, depending on the Catalog type.

#### Hadoop Catalog
* Make sure Hadoop is running.
* Set Catalog type as `hadoop` by setting VM option `-DCATALOG_TYPE=hadoop`
* Run [**SparkIcebergApplication**](src/main/java/com/ksoot/spark/iceberg/SparkIcebergApplication.java) as Spring boot application.

#### Hive Catalog
* Make sure Hadoop is running.
* Make sure Hive Server and Hive Metastore are running as per [Hadoop & Hive Installation Guide](https://medium.com/@officiallysingh/install-apache-hadoop-and-hive-on-mac-m3-7933e509da90).
* Make sure Postgres is running with same username and password as specified in `$HIVE_HOME/conf/hive-site.xml`.
* Set Catalog type as `hive` by setting VM option `-DCATALOG_TYPE=hive`
* Run [**SparkIcebergApplication**](src/main/java/com/ksoot/spark/iceberg/SparkIcebergApplication.java) as Spring boot application.

#### Nessie Catalog
* Make sure Hadoop is running.
* Make sure MongoDB and Nessie are running. Confirm that Mongo connection-string is set correctly in environment variable `quarkus.mongodb.connection-string` in [compose.yml](compose.yml)'s service `nessie`. Recommended to run Mongo and Nessie using docker-compose.
* You can access Nessie UI at `http://localhost:19120/api/v2/ui` to view the Iceberg tables.
* Set Catalog type as `nessie` by setting VM option `-DCATALOG_TYPE=nessie`
* Run [**SparkIcebergApplication**](src/main/java/com/ksoot/spark/iceberg/SparkIcebergApplication.java) as Spring boot application.

### With AWS S3 as Storage
* Make sure the required setup for AWS S3 is complete as elaborated in [Setting up AWS S3 for Data storage section](#setting-up-aws-s3-for-data-storage).
* Got to main class [**SparkIcebergApplication**](src/main/java/com/ksoot/spark/iceberg/SparkIcebergApplication.java) and Modify run configurations as follows.
* Set Storage type as `aws-s3` by setting VM option `-DSTORAGE_TYPE=aws-s3`
* Set VM option `-DCATALOG_AWS_BUCKET=<Your S3 Bucket Name>` to specify your bucket name.
* Set VM options `-DAWS_ACCESS_KEY=<Your access key>` and `-DAWS_SECRET_KEY=<Your secret key>` to specify the AWS credentials.
* Set Catalog type as follows.

#### Hadoop Catalog
* Make sure Hadoop is installed and running on your machine as per [Hadoop Installation Guide](https://medium.com/@officiallysingh/install-apache-hadoop-and-hive-on-mac-m3-7933e509da90).
* Set Catalog type as `hadoop` by setting VM option `-DCATALOG_TYPE=hadoop`
* Run [**SparkIcebergApplication**](src/main/java/com/ksoot/spark/iceberg/SparkIcebergApplication.java) as Spring boot application.

#### Hive Catalog
* Make sure Hive Server and Hive Metastore are running as per [Hadoop & Hive Installation Guide](https://medium.com/@officiallysingh/install-apache-hadoop-and-hive-on-mac-m3-7933e509da90).
* Make sure Postgres is running with same username and password as specified in `$HIVE_HOME/conf/hive-site.xml`.
* Set Catalog type as `hive` by setting VM option `-DCATALOG_TYPE=hive`
* Run [**SparkIcebergApplication**](src/main/java/com/ksoot/spark/iceberg/SparkIcebergApplication.java) as Spring boot application.

#### Nessie Catalog
* Make sure MongoDB and Nessie are running. Confirm that Mongo connection-string is set correctly in environment variable `quarkus.mongodb.connection-string` in [compose.yml](compose.yml)'s service `nessie`. Recommended to run Mongo and Nessie using docker-compose.
* You can access Nessie UI at `http://localhost:19120/api/v2/ui` to view the Iceberg tables.
* Set Catalog type as `nessie` by setting VM option `-DCATALOG_TYPE=nessie`
* Run [**SparkIcebergApplication**](src/main/java/com/ksoot/spark/iceberg/SparkIcebergApplication.java) as Spring boot application.

## Testing
Once the application is running, you can access [Swagger UI](http://localhost:8090/swagger-ui/index.html) or Using [Postman Collection](Spark Iceberg.postman_collection.json).

![Swagger UI](https://github.com/officiallysingh/spring-boot-spark-iceberg/blob/main/img/Swagger.png)

For Demo purpose, two tables `driver_hourly_stats` and `customer_daily_profile` are created in `ksoot` namespace.  
And data is written and read from these two tables using REST API endpoints.
* Execute the API to create or update the tables.
* Execute the API to print schema of the tables.
* Execute the API to write data to the tables.
* Execute the API to read data from the tables.
* Execute the API to delete the tables if you want to clean up.

## Licence
Open source [**The MIT License**](http://www.opensource.org/licenses/mit-license.php)

## Author
[**Rajveer Singh**](https://www.linkedin.com/in/rajveer-singh-589b3950/), In case you find any issues or need any support, please email me at raj14.1984@gmail.com.
Give it a :star: on [Github](https://github.com/officiallysingh/spring-boot-spark-iceberg) and a :clap: on [**medium.com**](https://officiallysingh.medium.com/spark-spring-boot-starter-e206def765b9) if you find it helpful.

## References
- [Spring boot starter for Spark](https://github.com/officiallysingh/spring-boot-starter-spark).
- [Apache Hadoop and Hive installation guide](https://medium.com/@officiallysingh/install-apache-hadoop-and-hive-on-mac-m3-7933e509da90) for details on how to install Hadoop and Hive.
- To know about Spark Refer to [**Spark Documentation**](https://spark.apache.org/docs/latest/).
- Find all Spark Configurations details at [**Spark Configuration Documentation**](https://spark.apache.org/docs/latest/configuration.html)
- [Apache Iceberg](https://iceberg.apache.org/docs/nightly/)
- [Apache Iceberg Spark Quickstart](https://iceberg.apache.org/docs/1.9.0/java-api-quickstart/)
- [Apache Hadoop](https://hadoop.apache.org/)
- [Apache Hive](https://hive.apache.org/)
- [Nessie](https://projectnessie.org/iceberg/iceberg/)
- [Exception handling in Spring boot Web applications](https://github.com/officiallysingh/spring-boot-problem-handler).
