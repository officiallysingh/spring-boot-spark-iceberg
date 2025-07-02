package com.ksoot.spark.iceberg.service;

import static com.ksoot.spark.iceberg.util.Constants.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_timestamp;

import com.ksoot.spark.util.SparkOptions;
import com.ksoot.spark.util.SparkUtils;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class SparkIcebergDataService {

  private final SparkSession sparkSession;

  private final IcebergCatalogClient icebergCatalogClient;

  private final DriverHourlyStatsGenerator driverHourlyStatsGenerator;

  private final CustomerDailyProfileGenerator customerDailyProfileGenerator;

  private final SparkIcebergService sparkIcebergService;

  // Number of Drivers to generate data for
  private static final int DRIVERS_COUNT = 5;

  // Number of Drivers to generate data for
  private static final int CUSTOMERS_COUNT = 5;

  // Number of days for which to produce data.
  // 180 days means generate data for the last 6 months
  private static final int DURATION_DAYS = 30;

  public void writeData() throws NoSuchTableException {
    this.writeDriversHourlyStatsData();
    //    this.writeCustomersDailyProfilesData();
  }

  public void writeDriversHourlyStatsData() throws NoSuchTableException {
    Dataset<Row> dataset =
        this.driverHourlyStatsGenerator.generateDriverStatsDataset(DRIVERS_COUNT, DURATION_DAYS);
    SparkUtils.logDataset("Driver Stats", dataset);
    this.sparkIcebergService.createData(dataset, TABLE_NAME_DRIVER_HOURLY_STATS);
    //    this.sparkIcebergService.appendData(dataset, TABLE_NAME_DRIVER_HOURLY_STATS);
  }

  public void dumpDriversHourlyStatsData() {
    Dataset<Row> dataset =
        this.driverHourlyStatsGenerator.generateDriverStatsDataset(DRIVERS_COUNT, DURATION_DAYS);
    SparkUtils.logDataset("Driver Stats", dataset);
    dataset.coalesce(1).write().option(SparkOptions.Common.HEADER, true).csv("driver-stats.csv");
  }

  public void executeSparkPipeline() throws NoSuchTableException {
    Dataset<Row> dataset =
        this.sparkSession
            .read()
            .option(SparkOptions.Common.HEADER, true)
            .csv("data/driver_stats.csv");
    //    final String schema = dataset.schema().treeString();
    // Convert event_timestamp from STRING to TIMESTAMP
    dataset =
        dataset
            .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
            .withColumn("created", to_timestamp(col("created")))
            .withColumn("driver_id", col("driver_id").cast("long"))
            .withColumn("conv_rate", col("conv_rate").cast("float"))
            .withColumn("acc_rate", col("acc_rate").cast("float"))
            .withColumn("avg_daily_trips", col("avg_daily_trips").cast("int"));

    SparkUtils.logDataset("Driver Stats", dataset);
    //    this.sparkIcebergService.appendData(dataset, TABLE_NAME_DRIVER_HOURLY_STATS);
    //    this.sparkIcebergService.createData(dataset, TABLE_NAME_DRIVER_HOURLY_STATS);
    dataset.write().mode("Overwrite").csv("data/driver-stats-processed.csv");
  }

  public void writeCustomersDailyProfilesData() throws NoSuchTableException {
    Dataset<Row> dataset =
        this.customerDailyProfileGenerator.generateDriverStatsDataset(
            CUSTOMERS_COUNT, DURATION_DAYS);
    SparkUtils.logDataset("Customer Profiles", dataset);
    this.sparkIcebergService.appendData(dataset, TABLE_NAME_CUSTOMER_DAILY_PROFILE);
  }

  public void readData(final String tableName) {
    Dataset<Row> dataset = this.sparkIcebergService.read(tableName);
    SparkUtils.logDataset(tableName, dataset);
  }
}
