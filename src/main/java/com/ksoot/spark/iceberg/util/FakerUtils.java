package com.ksoot.spark.iceberg.util;

import java.time.LocalTime;
import net.datafaker.Faker;

public class FakerUtils {

  public static LocalTime randomLocalTime(final Faker faker) {
    // Generate random hour (0-23), minute (0-59), second (0-59)
    int hour = faker.number().numberBetween(0, 24);
    int minute = faker.number().numberBetween(0, 60);
    int second = faker.number().numberBetween(0, 60);
    int nanoOfSecond = faker.number().numberBetween(0, 999);

    return LocalTime.of(hour, minute, second, nanoOfSecond);
  }
}
