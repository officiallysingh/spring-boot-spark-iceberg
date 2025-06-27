package com.ksoot.spark.iceberg;

import static com.ksoot.spark.iceberg.util.Constants.TABLE_NAME_CUSTOMER_DAILY_PROFILE;
import static com.ksoot.spark.iceberg.util.Constants.TABLE_NAME_DRIVER_HOURLY_STATS;

import com.ksoot.spark.iceberg.model.IcebergField;
import com.ksoot.spark.iceberg.service.IcebergCatalogClient;
import com.ksoot.spark.iceberg.service.SparkIcebergDataService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasTypeDefHeader;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@Controller
@RequestMapping("/v1/iceberg")
@Tag(name = "Spark Iceberg", description = "APIs")
@Slf4j
@RequiredArgsConstructor
public class SparkIcebergController {

  private final IcebergCatalogClient icebergCatalogClient;

  private final SparkIcebergDataService sparkIcebergDataService;

  private final AtlasClientV2 atlasClient;

  @Operation(operationId = "atlas-test", summary = "Test Atlas Client")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Atlas Client test successful"),
        @ApiResponse(responseCode = "500", description = "Internal Server Error")
      })
  @PostMapping("/atlas/test")
  ResponseEntity<String> atlasTest() throws AtlasServiceException {
//    SearchFilter searchFilter = new SearchFilter();
//    searchFilter.setParam("typeName", "hive_db");
//    List<AtlasTypeDefHeader> atlasTypeDefHeaders =
//        this.atlasClient.getAllTypeDefHeaders(searchFilter);
//    AtlasTypesDef atlasTypeDefHeaders =
//        this.atlasClient.getAllTypeDefs(searchFilter);
//    AtlasSearchResult atlasSearchResult = this.atlasClient.basicSearch(
//            "hive_table",
//            null,
//            null,
//            null,
//            false,
//            10,
//            0);

    AtlasEntity.AtlasEntityWithExtInfo ksootHiveDb = this.atlasClient.getEntityByAttribute("hive_db", Map.of("qualifiedName", "ksoot@primary"));
    AtlasEntity.AtlasEntityWithExtInfo driverHourlyStatsTable = this.atlasClient.getEntityByAttribute("hive_table", Map.of("qualifiedName", "ksoot.driver_hourly_stats@primary"));
    return ResponseEntity.ok("Atlas client calls made successfully");
  }

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
    if (this.icebergCatalogClient.tableExists(TABLE_NAME_DRIVER_HOURLY_STATS)) {
      this.icebergCatalogClient.dropTable(TABLE_NAME_DRIVER_HOURLY_STATS);
    }
//    if (this.icebergCatalogClient.tableExists(TABLE_NAME_CUSTOMER_DAILY_PROFILE)) {
//      this.icebergCatalogClient.dropTable(TABLE_NAME_CUSTOMER_DAILY_PROFILE);
//    }
    return ResponseEntity.ok(
        "Iceberg Tables: "
            + TABLE_NAME_DRIVER_HOURLY_STATS
//            + " and "
//            + TABLE_NAME_CUSTOMER_DAILY_PROFILE
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
        icebergCatalogClient.createOrUpdateTable(
            TABLE_NAME_DRIVER_HOURLY_STATS,
            Set.of(IcebergField.required("driver_id", Types.LongType.get())),
            "event_timestamp",
            "created",
            Set.of(
                IcebergField.optional("conv_rate", Types.FloatType.get(), "Conversion rate"),
                IcebergField.optional("acc_rate", Types.FloatType.get(), "Acceptance rate"),
                IcebergField.optional("avg_daily_trips", Types.IntegerType.get(), "Average Daily Trips")));

//    Table customerProfileTable =
//        icebergCatalogClient.createOrUpdateTable(
//            TABLE_NAME_CUSTOMER_DAILY_PROFILE,
//            Set.of(IcebergField.required("customer_id", Types.LongType.get())),
//            "event_timestamp",
//            "created",
//            Set.of(
//                IcebergField.optional("current_balance", Types.FloatType.get(), "Current Balance"),
//                IcebergField.optional("avg_passenger_count", Types.FloatType.get(), "Average Passenger Count"),
//                IcebergField.optional("lifetime_trip_count", Types.IntegerType.get(), "Lifetime Trip Count")));

    return ResponseEntity.ok(
        "Iceberg Tables: "
            + TABLE_NAME_DRIVER_HOURLY_STATS
//            + " and "
//            + TABLE_NAME_CUSTOMER_DAILY_PROFILE
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
    Table driverStatsTable = this.icebergCatalogClient.loadTable(TABLE_NAME_DRIVER_HOURLY_STATS);
    String schemaRepresentationDriverStatsTable = driverStatsTable.schema().toString();
    System.out.println(
        "Schema Representation of Table "
            + TABLE_NAME_DRIVER_HOURLY_STATS
            + ": "
            + schemaRepresentationDriverStatsTable);

//    Table customerProfilesTable =
//        this.icebergCatalogClient.loadTable(TABLE_NAME_CUSTOMER_DAILY_PROFILE);
//    String schemaRepresentationCustomerProfilesTable = customerProfilesTable.schema().toString();
//    System.out.println(
//        "Schema Representation of Table "
//            + TABLE_NAME_CUSTOMER_DAILY_PROFILE
//            + ": "
//            + schemaRepresentationCustomerProfilesTable);
//    return ResponseEntity.ok(
//        List.of(schemaRepresentationDriverStatsTable, schemaRepresentationCustomerProfilesTable));
    return ResponseEntity.ok(
        List.of(schemaRepresentationDriverStatsTable));
  }

  @Operation(operationId = "dump-data", summary = "Dump Data into CSV files")
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "Data written to CSV files successfully"),
        @ApiResponse(responseCode = "400", description = "Bad request"),
        @ApiResponse(responseCode = "500", description = "Internal Server Error")
      })
  @PostMapping("/tables/dump-data")
  ResponseEntity<String> dumpData() {
    this.sparkIcebergDataService.dumpDriversHourlyStatsData();
    return ResponseEntity.ok(
        "Data written to CSV files successfully");
  }

  @Operation(operationId = "spark-pipeline", summary = "Execute Spark pipeline to read data from CSV and write into Iceberg Tables")
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "200",
            description = "Data read from CSV file and Written to Iceberg table successfully"),
        @ApiResponse(responseCode = "400", description = "Bad request"),
        @ApiResponse(responseCode = "500", description = "Internal Server Error")
      })
  @PostMapping("/spark/pipeline")
  ResponseEntity<String> executeSparkPipeline() throws NoSuchTableException {
    this.sparkIcebergDataService.executeSparkPipeline();
    return ResponseEntity.ok(
        "Data written to CSV files successfully");
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
  ResponseEntity<String> writeDate() throws NoSuchTableException {
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
//    this.sparkIcebergDataService.readData(TABLE_NAME_CUSTOMER_DAILY_PROFILE);
    return ResponseEntity.ok(
        "Data read from Iceberg Tables: "
            + TABLE_NAME_DRIVER_HOURLY_STATS
//            + " and "
//            + TABLE_NAME_CUSTOMER_DAILY_PROFILE
            + " successfully. Look at the logs for details");
  }
}
