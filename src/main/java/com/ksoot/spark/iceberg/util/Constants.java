package com.ksoot.spark.iceberg.util;

import lombok.experimental.UtilityClass;

@UtilityClass
public class Constants {
  public static final char BACKTICK = '`';
  public static final char QUOTE = '"';
  public static final String REGEX_STRING_BEFORE_SLASH = ".*/";
  public static final String REGEX_SPACE = "\\s+";
  public static final String SLASH = "/";
  public static final String SPACE = " ";
  public static final String BLANK = "";
  public static final String COMMA = ",";
  public static final String COLON = ": ";
  public static final String EQUAL_TO = " = ";
  public static final String OR = " OR ";
  public static final String AND = " AND ";
  public static final String IN = " IN ";
  public static final String UNDERSCORE = "_";
  public static final String DOT = ".";

  public static final String TABLE_NAME_DRIVER_HOURLY_STATS = "driver_hourly_stats";

  public static final String TABLE_NAME_CUSTOMER_DAILY_PROFILE = "customer_daily_profile";

  public static final String PROVIDER_ICEBERG = "iceberg";
}
