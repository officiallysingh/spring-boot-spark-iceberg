package com.ksoot.spark.iceberg.service;

import static com.ksoot.spark.iceberg.util.Constants.PROVIDER_ICEBERG;

import com.ksoot.spark.springframework.boot.autoconfigure.CatalogProperties;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.springframework.util.Assert;

@RequiredArgsConstructor
public class SparkIcebergService {

  private final CatalogProperties catalogProperties;

  private final SparkSession sparkSession;

  /**
   * Append the contents of the data frame to the output table.
   *
   * <p>If the output table does not exist, this operation will fail with
   * [[org.apache.spark.sql.catalyst.analysis.NoSuchTableException]]. The data frame will be
   * validated to ensure it is compatible with the existing table.
   *
   * @throws NoSuchTableException If the table does not exist
   */
  public void appendData(final Dataset<Row> dataset, final String tableName)
      throws NoSuchTableException {
    Assert.hasText(tableName, "'tableName' is required");
    final String icebergTable = this.icebergTableName(tableName);
    dataset.writeTo(icebergTable).append();
  }

  /**
   * Replace an existing table with the contents of the data frame.
   *
   * <p>The existing table's schema, partition layout, properties, and other configuration will be
   * replaced with the contents of the data frame and the configuration set on this writer.
   *
   * <p>If the output table does not exist, this operation will fail with
   * [[org.apache.spark.sql.catalyst.analysis.CannotReplaceMissingTableException]].
   */
  @Deprecated
  public void replaceData(final Dataset<Row> dataset, final String tableName) {
    Assert.hasText(tableName, "'tableName' is required");
    final String icebergTable = this.icebergTableName(tableName);
    dataset.writeTo(icebergTable).replace();
  }

  /**
   * Create a new table from the contents of the data frame.
   *
   * <p>The new table's schema, partition layout, properties, and other configuration will be based
   * on the configuration set on this writer.
   *
   * <p>If the output table exists, this operation will fail with
   * [[org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException]].
   */
  @Deprecated
  public void createData(final Dataset<Row> dataset, final String tableName) {
    Assert.hasText(tableName, "'tableName' is required");
    final String icebergTable = this.icebergTableName(tableName);
    dataset.writeTo(icebergTable).create();
  }

  /**
   * Create a new table or replace an existing table with the contents of the data frame.
   *
   * <p>The output table's schema, partition layout, properties, and other configuration will be
   * based on the contents of the data frame and the configuration set on this writer. If the table
   * exists, its configuration and data will be replaced.
   */
  @Deprecated
  public void createOrReplaceData(final Dataset<Row> dataset, final String tableName) {
    Assert.hasText(tableName, "'tableName' is required");
    final String icebergTable = this.icebergTableName(tableName);
    dataset.writeTo(icebergTable).createOrReplace();
  }

  public Dataset<Row> read(final String tableName) {
    Dataset<Row> dataset =
        this.sparkSession.read().format(PROVIDER_ICEBERG).table(this.icebergTableName(tableName));
    //    SparkUtils.logDataset(tableName, dataset);
    return dataset;
  }

  private String icebergTableName(final String tableName) {
    final String icebergTable =
        tableName.startsWith(this.catalogProperties.tablePrefix())
            ? tableName
            : this.catalogProperties.tablePrefix() + tableName;
    return icebergTable;
  }
}
