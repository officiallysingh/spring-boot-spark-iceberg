package com.ksoot.spark.iceberg.service;

import static com.ksoot.spark.iceberg.util.Constants.*;

import com.ksoot.spark.iceberg.util.SparkUtils;
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
    this.writeCustomersDailyProfilesData();
  }

  public void writeDriversHourlyStatsData() throws NoSuchTableException {
    Dataset<Row> dataset =
        this.driverHourlyStatsGenerator.generateDriverStatsDataset(DRIVERS_COUNT, DURATION_DAYS);
    SparkUtils.logDataset("Driver Stats", dataset);
    this.sparkIcebergService.appendData(dataset, TABLE_NAME_DRIVER_HOURLY_STATS);
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
