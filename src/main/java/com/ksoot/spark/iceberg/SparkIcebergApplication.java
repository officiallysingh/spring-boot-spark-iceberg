package com.ksoot.spark.iceberg;

import java.util.Locale;
import net.datafaker.Faker;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class SparkIcebergApplication {

  public static void main(String[] args) {
    SpringApplication.run(SparkIcebergApplication.class, args);
  }

  @Bean
  Faker faker() {
    return new Faker(new Locale.Builder().setLanguage("en").setRegion("US").build());
  }
}
