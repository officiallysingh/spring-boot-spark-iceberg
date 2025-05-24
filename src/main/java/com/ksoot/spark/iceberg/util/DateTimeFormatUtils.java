package com.ksoot.spark.iceberg.util;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import lombok.experimental.UtilityClass;

@UtilityClass
public class DateTimeFormatUtils {

  //  public static final String SPARK_DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSXXX";
  public static final String SPARK_DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

  // To use in mappers
  public static LocalDateTime toLocalDateTime(final LocalDate localDate) {
    return Objects.nonNull(localDate) ? localDate.atStartOfDay() : null;
  }

  // To use in mappers
  public static LocalDate toLocalDate(final LocalDateTime localDateTime) {
    return Objects.nonNull(localDateTime) ? localDateTime.toLocalDate() : null;
  }

  public static String format(final LocalDate date) {
    return date.format(DateTimeFormatter.ISO_DATE);
  }

  public static String format(final LocalDate date, final String pattern) {
    return date.format(DateTimeFormatter.ofPattern(pattern));
  }

  public static String format(final LocalDate date, final DateTimeFormatter formatter) {
    return date.format(formatter);
  }

  public static String format(final LocalDateTime datetime) {
    return datetime.format(DateTimeFormatter.ISO_DATE_TIME);
  }

  public static String format(final LocalDateTime datetime, final String pattern) {
    return datetime.format(DateTimeFormatter.ofPattern(pattern));
  }

  public static String format(final LocalDateTime datetime, final DateTimeFormatter formatter) {
    return datetime.format(formatter);
  }

  public static String format(final ZonedDateTime zonedDateTime) {
    return zonedDateTime.format(DateTimeFormatter.ISO_ZONED_DATE_TIME);
  }

  public static String format(final ZonedDateTime zonedDateTime, final String pattern) {
    return zonedDateTime.format(DateTimeFormatter.ofPattern(pattern));
  }

  public static String format(
      final ZonedDateTime zonedDateTime, final DateTimeFormatter formatter) {
    return zonedDateTime.format(formatter);
  }

  public static String format(final OffsetDateTime offsetDateTime) {
    return offsetDateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
  }

  public static String format(final OffsetDateTime offsetDateTime, final String pattern) {
    return offsetDateTime.format(DateTimeFormatter.ofPattern(pattern));
  }

  public static String format(
      final OffsetDateTime offsetDateTime, final DateTimeFormatter formatter) {
    return offsetDateTime.format(formatter);
  }

  public static String convertAndFormat(
      final OffsetDateTime sourceOffsetDateTime, final ZoneOffset targetZoneId) {
    return sourceOffsetDateTime.getOffset() == targetZoneId
        ? format(sourceOffsetDateTime)
        : format(sourceOffsetDateTime.withOffsetSameInstant(targetZoneId));
  }

  public static String convertAndFormat(
      final ZonedDateTime sourceZoneDatetime, final ZoneId targetZoneId) {
    return sourceZoneDatetime.getZone() == targetZoneId
        ? format(sourceZoneDatetime)
        : format(sourceZoneDatetime.withZoneSameInstant(targetZoneId));
  }

  public static String format(final LocalTime time, final DateTimeFormatter formatter) {
    return time.format(formatter);
  }

  public static String format(final LocalTime time) {
    return time.format(DateTimeFormatter.ISO_DATE_TIME);
  }
}
