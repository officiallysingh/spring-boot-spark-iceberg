package com.ksoot.spark.iceberg.service;

import com.ksoot.spark.iceberg.util.FakerUtils;
import com.ksoot.spark.iceberg.util.SparkUtils;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import net.datafaker.Faker;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
public class DriverHourlyStatsGenerator {

  private final SparkSession sparkSession;

  private final Faker faker;

  private static final int START_DRIVER_ID = 1000;

  public Dataset<Row> generateDriverStatsDataset(int driversCount, final int durationDays) {
    // Generate data for the last 90 days
    final LocalDate toDate = LocalDate.now();
    final LocalDate fromDate = toDate.minusDays(durationDays);
    StructType schema =
        new StructType()
            .add("event_timestamp", DataTypes.TimestampType, false)
            .add("created", DataTypes.TimestampType, false)
            .add("driver_id", DataTypes.LongType, false)
            .add("conv_rate", DataTypes.FloatType, false)
            .add("acc_rate", DataTypes.FloatType, false)
            .add("avg_daily_trips", DataTypes.IntegerType, false);

    final List<Row> data =
        IntStream.range(START_DRIVER_ID, START_DRIVER_ID + driversCount)
            .mapToObj(String::valueOf)
            .map(
                driverId ->
                    Stream.iterate(
                            fromDate,
                            date -> !date.isAfter(toDate.minusDays(1)),
                            date -> date.plusDays(1))
                        .flatMap(date -> this.generateDriverStatsForADay(driverId, date).stream())
                        .toList())
            .flatMap(Collection::stream)
            .toList();
    return this.sparkSession.createDataFrame(data, schema);
  }

  private List<Row> generateDriverStatsForADay(final String driverId, final LocalDate date) {
    final int count = this.faker.number().randomDigitNotZero();
    return IntStream.range(0, count)
        .mapToObj(i -> this.generateDriverStatsRow(driverId, date))
        .toList();
  }

  private Row generateDriverStatsRow(final String driverId, final LocalDate date) {
    final float convRate = (float) this.faker.number().randomDouble(3, 200, 900);
    final float accRate = (float) this.faker.number().randomDouble(3, 300, 800);
    final int avgDailyTrips = (int) this.faker.number().randomNumber(3, true);
    final LocalDateTime timestamp = date.atTime(FakerUtils.randomLocalTime(faker));
    final Timestamp eventTimestamp = SparkUtils.toSparkTimestamp(timestamp);
    final Timestamp createdTimestamp =
        SparkUtils.toSparkTimestamp(timestamp.toLocalDate().atStartOfDay().plusHours(12));
    return RowFactory.create(
        eventTimestamp, createdTimestamp, Long.valueOf(driverId), convRate, accRate, avgDailyTrips);
  }
}
