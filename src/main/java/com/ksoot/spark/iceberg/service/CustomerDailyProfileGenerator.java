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
public class CustomerDailyProfileGenerator {

  private final SparkSession sparkSession;

  private final Faker faker;

  private static final int START_CUSTOMER_ID = 200;

  public Dataset<Row> generateDriverStatsDataset(int customersCount, final int durationDays) {
    // Generate data for the last 90 days
    final LocalDate toDate = LocalDate.now();
    final LocalDate fromDate = toDate.minusDays(durationDays);
    StructType schema =
        new StructType()
            .add("event_timestamp", DataTypes.TimestampType, false)
            .add("created", DataTypes.TimestampType, false)
            .add("customer_id", DataTypes.LongType, false)
            .add("current_balance", DataTypes.FloatType, false)
            .add("avg_passenger_count", DataTypes.FloatType, false)
            .add("lifetime_trip_count", DataTypes.IntegerType, false);
    final List<Row> data =
        IntStream.range(START_CUSTOMER_ID, START_CUSTOMER_ID + customersCount)
            .mapToObj(String::valueOf)
            .map(
                customerId ->
                    Stream.iterate(
                            fromDate,
                            date -> !date.isAfter(toDate.minusDays(1)),
                            date -> date.plusDays(1))
                        .map(date -> this.generateDriverStatsRow(customerId, date))
                        .toList())
            .flatMap(Collection::stream)
            .toList();
    return this.sparkSession.createDataFrame(data, schema);
  }

  private Row generateDriverStatsRow(final String customerId, final LocalDate date) {
    final float currentBalance = (float) this.faker.number().randomDouble(3, 200, 900);
    final float avgPassengerCount = (float) this.faker.number().randomDouble(3, 300, 800);
    final int lifetimeTripCount = (int) this.faker.number().randomNumber(3, true);
    final LocalDateTime timestamp = date.atTime(FakerUtils.randomLocalTime(faker));
    final Timestamp eventTimestamp = SparkUtils.toSparkTimestamp(timestamp);
    final Timestamp createdTimestamp =
        SparkUtils.toSparkTimestamp(timestamp.toLocalDate().atStartOfDay().plusHours(12));
    return RowFactory.create(
        eventTimestamp,
        createdTimestamp,
        Long.valueOf(customerId),
        currentBalance,
        avgPassengerCount,
        lifetimeTripCount);
  }
}
