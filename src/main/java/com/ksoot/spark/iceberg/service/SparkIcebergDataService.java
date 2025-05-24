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

  private final IcebergClient icebergClient;

  private final DriverHourlyStatsGenerator driverHourlyStatsGenerator;

  private final CustomerDailyProfileGenerator customerDailyProfileGenerator;

  // Number of Drivers to generate data for
  private static final int DRIVERS_COUNT = 5;

  // Number of Drivers to generate data for
  private static final int CUSTOMERS_COUNT = 5;

  // Number of days for which to produce data.
  // 180 days means generate data for the last 6 months
  private static final int DURATION_DAYS = 30;

  public void writeData() {
    //    IcebergClient icebergClient = this.icebergClientProvider.getIfAvailable();
    //    if(Objects.nonNull(icebergClient)) {
    //      Table driverHourlyStatsTable = icebergClient.loadTable(TABLE_NAME_DRIVER_HOURLY_STATS);
    //      System.out.println(driverHourlyStatsTable);
    //    } else {
    //      System.out.println("The Iceberg Client is not available");
    //    }
    try {
      //              this.testWriteIceberg();
      this.writeDriversHourlyStatsData();
      this.writeCustomersDailyProfilesData();
    } catch (NoSuchTableException e) {
      throw new RuntimeException(e);
    }
  }

  public void writeDriversHourlyStatsData() throws NoSuchTableException {
    Dataset<Row> dataset =
        this.driverHourlyStatsGenerator.generateDriverStatsDataset(DRIVERS_COUNT, DURATION_DAYS);
    SparkUtils.logDataset("Driver Stats", dataset);
    //    dataset
    //        .writeTo(this.icebergTableName(TABLE_NAME_DRIVER_HOURLY_STATS))
    //        .using(PROVIDER_ICEBERG)
    //        .createOrReplace();
    //    dataset.writeTo(TABLE_NAME_DRIVER_HOURLY_STATS).option("format", "iceberg").append();
    dataset.writeTo(this.icebergTableName(TABLE_NAME_DRIVER_HOURLY_STATS)).append();
  }

  public void writeCustomersDailyProfilesData() throws NoSuchTableException {
    Dataset<Row> dataset =
        this.customerDailyProfileGenerator.generateDriverStatsDataset(
            CUSTOMERS_COUNT, DURATION_DAYS);
    SparkUtils.logDataset("Customer Profiles", dataset);
    dataset
        .writeTo(this.icebergTableName(TABLE_NAME_CUSTOMER_DAILY_PROFILE))
        .using(PROVIDER_ICEBERG)
        .createOrReplace();
    //    dataset.writeTo(TABLE_NAME_CUSTOMER_DAILY_PROFILE).option("format", "iceberg").append();
    //    dataset.writeTo(this.icebergTableName(TABLE_NAME_CUSTOMER_DAILY_PROFILE)).option("format",
    // "iceberg").append();
  }

  public void readData(final String tableName) {
    Dataset<Row> dataset =
        this.sparkSession.read().format(PROVIDER_ICEBERG).table(this.icebergTableName(tableName));
    SparkUtils.logDataset(tableName, dataset);
  }

  private String icebergTableName(final String tableName) {
    return this.icebergClient.getCatalogProperties().tablePrefix() + tableName;
  }
}
