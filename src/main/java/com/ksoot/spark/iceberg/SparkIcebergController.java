package com.ksoot.spark.iceberg;

import static com.ksoot.spark.iceberg.util.Constants.TABLE_NAME_CUSTOMER_DAILY_PROFILE;
import static com.ksoot.spark.iceberg.util.Constants.TABLE_NAME_DRIVER_HOURLY_STATS;

import com.ksoot.spark.iceberg.model.IcebergField;
import com.ksoot.spark.iceberg.service.IcebergClient;
import com.ksoot.spark.iceberg.service.SparkIcebergDataService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.List;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@Controller
@RequestMapping("/v1/iceberg")
@Tag(name = "Spark Iceberg", description = "APIs")
@Slf4j
@RequiredArgsConstructor
public class SparkIcebergController {

  private final IcebergClient icebergClient;

  private final SparkIcebergDataService sparkIcebergDataService;

  @Operation(operationId = "delete-tables", summary = "Delete Iceberg Tables")
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "Create or Update Entity request accepted successfully"),
        @ApiResponse(responseCode = "400", description = "Bad request"),
        @ApiResponse(responseCode = "500", description = "Internal Server Error")
      })
  @DeleteMapping("/tables")
  ResponseEntity<String> deleteTables() {
    if (this.icebergClient.tableExists(TABLE_NAME_DRIVER_HOURLY_STATS)) {
      this.icebergClient.dropTable(TABLE_NAME_DRIVER_HOURLY_STATS);
    }
    if (this.icebergClient.tableExists(TABLE_NAME_CUSTOMER_DAILY_PROFILE)) {
      this.icebergClient.dropTable(TABLE_NAME_CUSTOMER_DAILY_PROFILE);
    }
    return ResponseEntity.ok(
        "Iceberg Tables: "
            + TABLE_NAME_DRIVER_HOURLY_STATS
            + " and "
            + TABLE_NAME_CUSTOMER_DAILY_PROFILE
            + " deleted successfully");
  }

  @Operation(operationId = "create-update-tables", summary = "Create or Update Iceberg Tables")
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "Created or Updated Iceberg Tables successfully"),
        @ApiResponse(responseCode = "400", description = "Bad request"),
        @ApiResponse(responseCode = "500", description = "Internal Server Error")
      })
  @PutMapping("/tables")
  ResponseEntity<String> createOrUpdateTables() {

    Table driverStatsTable =
        icebergClient.createOrUpdateTable(
            TABLE_NAME_DRIVER_HOURLY_STATS,
            Set.of(IcebergField.required("driver_id", Types.LongType.get())),
            "event_timestamp",
            "created",
            Set.of(
                IcebergField.optional("conv_rate", Types.FloatType.get()),
                IcebergField.optional("acc_rate", Types.FloatType.get()),
                IcebergField.optional("avg_daily_trips", Types.IntegerType.get())));

    Table customerProfileTable =
        icebergClient.createOrUpdateTable(
            TABLE_NAME_CUSTOMER_DAILY_PROFILE,
            Set.of(IcebergField.required("customer_id", Types.LongType.get())),
            "event_timestamp",
            "created",
            Set.of(
                IcebergField.optional("current_balance", Types.FloatType.get()),
                IcebergField.optional("avg_passenger_count", Types.FloatType.get()),
                IcebergField.optional("lifetime_trip_count", Types.IntegerType.get())));

    return ResponseEntity.ok(
        "Iceberg Tables: "
            + TABLE_NAME_DRIVER_HOURLY_STATS
            + " and "
            + TABLE_NAME_CUSTOMER_DAILY_PROFILE
            + " created successfully");
  }

  @Operation(operationId = "print-schema", summary = "Print Iceberg Tables Schema")
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "Printed Iceberg Tables Schema successfully"),
        @ApiResponse(responseCode = "400", description = "Bad request"),
        @ApiResponse(responseCode = "500", description = "Internal Server Error")
      })
  @GetMapping("/tables/print-schema")
  ResponseEntity<List<String>> printSchema() {
    Table driverStatsTable = this.icebergClient.loadTable(TABLE_NAME_DRIVER_HOURLY_STATS);
    String schemaRepresentationDriverStatsTable = driverStatsTable.schema().toString();
    System.out.println(
        "Schema Representation of Table "
            + TABLE_NAME_DRIVER_HOURLY_STATS
            + ": "
            + schemaRepresentationDriverStatsTable);

    Table customerProfilesTable = this.icebergClient.loadTable(TABLE_NAME_CUSTOMER_DAILY_PROFILE);
    String schemaRepresentationCustomerProfilesTable = customerProfilesTable.schema().toString();
    System.out.println(
        "Schema Representation of Table "
            + TABLE_NAME_CUSTOMER_DAILY_PROFILE
            + ": "
            + schemaRepresentationCustomerProfilesTable);
    return ResponseEntity.ok(
        List.of(schemaRepresentationDriverStatsTable, schemaRepresentationCustomerProfilesTable));
  }

  @Operation(operationId = "write-data", summary = "Ingest Data into Iceberg Tables")
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "Data ingested into Iceberg Tables successfully"),
        @ApiResponse(responseCode = "400", description = "Bad request"),
        @ApiResponse(responseCode = "500", description = "Internal Server Error")
      })
  @PostMapping("/tables/write-data")
  ResponseEntity<String> writeDate() {
    this.sparkIcebergDataService.writeData();
    return ResponseEntity.ok(
        "Data ingested into Iceberg Tables: "
            + TABLE_NAME_DRIVER_HOURLY_STATS
            + " and "
            + TABLE_NAME_CUSTOMER_DAILY_PROFILE
            + " successfully");
  }

  @Operation(operationId = "read-data", summary = "Read Data from Iceberg Tables.")
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "Data read from Iceberg Table successfully"),
        @ApiResponse(responseCode = "400", description = "Bad request"),
        @ApiResponse(responseCode = "500", description = "Internal Server Error")
      })
  @GetMapping("/tables/read-data")
  ResponseEntity<String> readDate() {
    this.sparkIcebergDataService.readData(TABLE_NAME_DRIVER_HOURLY_STATS);
    this.sparkIcebergDataService.readData(TABLE_NAME_CUSTOMER_DAILY_PROFILE);
    return ResponseEntity.ok(
        "Data read from Iceberg Tables: "
            + TABLE_NAME_DRIVER_HOURLY_STATS
            + " and "
            + TABLE_NAME_CUSTOMER_DAILY_PROFILE
            + " successfully. Look at the logs for details");
  }
}
