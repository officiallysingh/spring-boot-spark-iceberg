package com.ksoot.spark.iceberg.service;

import com.ksoot.spark.iceberg.model.IcebergField;
import com.ksoot.spark.springframework.boot.autoconfigure.CatalogProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class IcebergCatalogClient {

  private final Catalog catalog;

  private final CatalogProperties catalogProperties;

  public String getCatalogName() {
    return this.catalog.name();
  }

  public Catalog getCatalog() {
    return this.catalog;
  }

  public CatalogProperties getCatalogProperties() {
    return this.catalogProperties;
  }

  private TableIdentifier tableIdentifier(final String name) {
    return TableIdentifier.of(this.catalogProperties.namespace(), name);
  }

  public boolean tableExists(final String name) {
    final TableIdentifier tableIdentifier = this.tableIdentifier(name);
    return this.tableExists(tableIdentifier);
  }

  public boolean tableExists(final TableIdentifier tableIdentifier) {
    return this.catalog.tableExists(tableIdentifier);
  }

  public Table loadTable(final String name) {
    final TableIdentifier tableIdentifier = this.tableIdentifier(name);
    return this.loadTable(tableIdentifier);
  }

  public Table loadTable(final TableIdentifier tableIdentifier) {
    return this.catalog.loadTable(tableIdentifier);
  }

  public Table createOrUpdateTable(
      final String name, final Set<IcebergField> keys, final Set<IcebergField> columns) {
    return this.createOrUpdateTable(name, keys, columns, null);
  }

  public Table createOrUpdateTable(
      final String name,
      final Set<IcebergField> keys,
      final String eventTimestamp,
      final String createdTimestamp,
      final Set<IcebergField> columns) {
    return this.createOrUpdateTable(name, keys, eventTimestamp, createdTimestamp, columns, null);
  }

  public Table createOrUpdateTable(
      final String name,
      final Set<IcebergField> keys,
      final Set<IcebergField> columns,
      final Map<String, String> properties) {
    return this.createOrUpdateTable(name, keys, null, null, columns, properties);
  }

  public Table createOrUpdateTable(
      final String name,
      final Set<IcebergField> keys,
      final String eventTimestamp,
      final String createdTimestamp,
      final Set<IcebergField> columns,
      final Map<String, String> properties) {

    final TableIdentifier tableIdentifier = this.tableIdentifier(name);

    // If Table already exists, check if the Schema has changed. If yes, update the Schema otherwise
    // do nothing.
    if (this.tableExists(tableIdentifier)) {
      // Validate if the Entity columns, Field names, eventTimestamp and createdTimestamp column
      // names are not conflicting
      this.validatePreconditions(keys, eventTimestamp, createdTimestamp, columns);
      final Table table = this.loadTable(tableIdentifier);

      final Schema existingSchema = table.schema();

      if (StringUtils.isNotBlank(eventTimestamp)) {
        final Types.NestedField existingEventTimestampField = existingSchema.findField(1);
        if (!existingEventTimestampField.name().equals(eventTimestamp)) {
          table
              .updateSchema()
              .renameColumn(existingEventTimestampField.name(), eventTimestamp)
              .commit();
        }
      }

      if (StringUtils.isNotBlank(createdTimestamp)) {
        final Types.NestedField existingCreatedTimestampField = existingSchema.findField(2);
        if (!existingCreatedTimestampField.name().equals(createdTimestamp)) {
          table
              .updateSchema()
              .renameColumn(existingCreatedTimestampField.name(), createdTimestamp)
              .commit();
        }
      }

      Map<String, Type> existingFields =
          existingSchema.columns().stream()
              .collect(Collectors.toMap(Types.NestedField::name, Types.NestedField::type));

      Map<String, Type> newFields =
          columns.stream()
              .map(column -> Pair.of(column.getName(), column.getType()))
              .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
      keys.forEach(
          key -> {
            newFields.put(key.getName(), key.getType());
          });

      // Check if the Schema has changed
      if (existingFields.equals(newFields)) {
        log.info("Iceberg Table: {} Schema not changed", name);
        return table;
      } else {
        for (final IcebergField key : keys) {
          if (!existingFields.containsKey(key.getName())) {
            table
                .updateSchema()
                .addRequiredColumn(key.getName(), key.getType(), key.getDescription())
                .commit();
            log.info("Field: {} is added to table: {} successfully", key.getName(), name);
          }
        }

        for (final IcebergField column : columns) {
          if (!existingFields.containsKey(column.getName())) {
            if (column.isOptional()) {
              table
                  .updateSchema()
                  .addColumn(column.getName(), column.getType(), column.getDescription())
                  .commit();
            } else {
              table
                  .updateSchema()
                  .addRequiredColumn(column.getName(), column.getType(), column.getDescription())
                  .commit();
            }
            log.info("field: {} is added to table: {} successfully", column.getName(), name);
          }
        }
        log.info("Iceberg Table: {} Schema updated successfully", name);
        return table;
      }
    } else {
      // Validate if the Entity columns, Field names, eventTimestamp and createdTimestamp column
      // names are not conflicting
      this.validatePreconditions(keys, eventTimestamp, createdTimestamp, columns);
      // Create new Table
      final Table table =
          this.createTable(name, keys, eventTimestamp, createdTimestamp, columns, properties);
      return table;
    }
  }

  private void validatePreconditions(
      Set<IcebergField> keys,
      String eventTimestamp,
      String createdTimestamp,
      Set<IcebergField> columns) {
    for (final IcebergField key : keys) {
      if (StringUtils.isBlank(key.getName())) {
        throw new IcebergClientException(
            "Entity key column name can not be blank or null: " + key.getName());
      }
      if (key.getName().equals(eventTimestamp)) {
        throw new IcebergClientException(
            "Conflicting Entity column name with given 'eventTimestamp' column name: "
                + eventTimestamp);
      }
      if (key.getName().equals(createdTimestamp)) {
        throw new IcebergClientException(
            "Conflicting Entity column name with given 'createdTimestamp' column name: "
                + createdTimestamp);
      }
      if (columns.stream().anyMatch(IcebergField -> IcebergField.getName().equals(key.getName()))) {
        throw new IcebergClientException(
            "Conflicting Field name with Entity column name: " + key.getName());
      }
    }
    if (columns.stream().map(IcebergField::getName).anyMatch(StringUtils::isBlank)) {
      throw new IcebergClientException("Field column name can not be blank or null");
    }

    if (StringUtils.isNotBlank(eventTimestamp)
        && columns.stream()
            .anyMatch(IcebergField -> IcebergField.getName().equals(eventTimestamp))) {
      throw new IcebergClientException(
          "Conflicting Field name with given 'eventTimestamp' column name: " + eventTimestamp);
    }
    if (StringUtils.isNotBlank(createdTimestamp)
        && columns.stream()
            .anyMatch(IcebergField -> IcebergField.getName().equals(createdTimestamp))) {
      throw new IcebergClientException(
          "Conflicting Field name with given 'createdTimestamp' column name: " + createdTimestamp);
    }
  }

  public Table createTable(
      final String name,
      final Set<IcebergField> keys,
      final String eventTimestamp,
      final String createdTimestamp,
      final Set<IcebergField> columns) {
    return this.createTable(name, keys, eventTimestamp, createdTimestamp, columns, null);
  }

  public Table createTable(
      final String name,
      final Set<IcebergField> keys,
      final String eventTimestamp,
      final String createdTimestamp,
      final Set<IcebergField> columns,
      final Map<String, String> properties) {

    final TableIdentifier tableIdentifier = this.tableIdentifier(name);

    final Schema tableSchema = this.buildSchema(keys, columns, eventTimestamp, createdTimestamp);
    final PartitionSpec partitionSpec = this.buildPartitionSpec(keys, tableSchema, eventTimestamp);

    final Table table =
        this.catalog.createTable(tableIdentifier, tableSchema, partitionSpec, properties);

    log.info("Iceberg Table: {} created successfully", name);
    return table;
  }

  public Table createTable(
      final String name,
      final Set<IcebergField> keys,
      final Set<IcebergField> columns,
      final Map<String, String> properties) {

    final TableIdentifier tableIdentifier = this.tableIdentifier(name);

    final Schema tableSchema = this.buildSchema(keys, columns);
    final PartitionSpec partitionSpec = this.buildPartitionSpec(keys, tableSchema);

    final Table table =
        this.catalog.createTable(tableIdentifier, tableSchema, partitionSpec, properties);

    log.info("Iceberg Table: {} created successfully", name);
    return table;
  }

  private Schema buildSchema(final Set<IcebergField> keys, final Set<IcebergField> columns) {
    return this.buildSchema(keys, columns, null, null);
  }

  private Schema buildSchema(
      final Set<IcebergField> keys,
      final Set<IcebergField> columns,
      final String eventTimestamp,
      final String createdTimestamp) {
    final List<Types.NestedField> icebergColumns = new ArrayList<>();
    int id = 1;

    if (StringUtils.isNotBlank(eventTimestamp)) {
      final Types.NestedField eventTimestampField =
          Types.NestedField.required(
              id++, eventTimestamp, Types.TimestampType.withZone(), "Event Timestamp");
      icebergColumns.add(eventTimestampField);
    }

    if (StringUtils.isNotBlank(createdTimestamp)) {
      final Types.NestedField createdTimestampField =
          Types.NestedField.optional(
              id++, createdTimestamp, Types.TimestampType.withZone(), "Created Timestamp");
      icebergColumns.add(createdTimestampField);
    }

    for (final IcebergField key : keys) {
      final Type entityColumnType = key.getType();
      final Types.NestedField entityField =
          Types.NestedField.required(id++, key.getName(), entityColumnType, "Entity Column");
      icebergColumns.add(entityField);
    }
    for (final IcebergField column : columns) {
      final Type FieldType = column.getType();
      final Types.NestedField FieldField =
          Types.NestedField.of(
              id++, column.isOptional(), column.getName(), FieldType, column.getDescription());
      icebergColumns.add(FieldField);
    }

    final Schema tableSchema =
        new Schema(icebergColumns.toArray(new Types.NestedField[icebergColumns.size()]));
    return tableSchema;
  }

  private PartitionSpec buildPartitionSpec(final Set<IcebergField> keys, final Schema tableSchema) {
    return this.buildPartitionSpec(keys, tableSchema, null);
  }

  private PartitionSpec buildPartitionSpec(
      final Set<IcebergField> keys, final Schema tableSchema, final String eventTimestamp) {
    final PartitionSpec.Builder partitionSpecBuilder = PartitionSpec.builderFor(tableSchema);
    keys.forEach(
        key -> {
          partitionSpecBuilder.identity(key.getName());
        });

    if (StringUtils.isNotEmpty(eventTimestamp)) {
      partitionSpecBuilder.hour(eventTimestamp);
    }

    final PartitionSpec partitionSpec = partitionSpecBuilder.build();
    return partitionSpec;
  }

  public Table createTable(
      final TableIdentifier tableIdentifier,
      final Schema schema,
      final PartitionSpec partitionSpec,
      final Map<String, String> properties) {
    if (this.tableExists(tableIdentifier)) {
      throw new IcebergClientException(
          "Iceberg table with name: " + tableIdentifier.name() + " already exists");
    }
    final Table table =
        this.catalog.createTable(tableIdentifier, schema, partitionSpec, properties);
    return table;
  }

  public Table createTable(
      final TableIdentifier tableIdentifier,
      final Schema schema,
      final PartitionSpec partitionSpec) {
    final Table table = this.catalog.createTable(tableIdentifier, schema, partitionSpec, null);
    return table;
  }

  public Table createTable(final TableIdentifier tableIdentifier, final Schema schema) {
    return this.createTable(tableIdentifier, schema, PartitionSpec.unpartitioned(), null);
  }

  public Table createTable(final Namespace namespace, final String name, final Schema schema) {
    final TableIdentifier tableIdentifier = TableIdentifier.of(namespace, name);
    return this.createTable(tableIdentifier, schema, PartitionSpec.unpartitioned(), null);
  }

  public Table createTable(
      final Namespace namespace,
      final String name,
      final Schema schema,
      final PartitionSpec partitionSpec) {
    final TableIdentifier tableIdentifier = TableIdentifier.of(namespace, name);
    return this.createTable(tableIdentifier, schema, partitionSpec, null);
  }

  public Table createTable(final String name, final Schema schema) {
    final TableIdentifier tableIdentifier = this.tableIdentifier(name);
    return this.createTable(tableIdentifier, schema, PartitionSpec.unpartitioned(), null);
  }

  public Table createTable(
      final String name, final Schema schema, final PartitionSpec partitionSpec) {
    final TableIdentifier tableIdentifier = this.tableIdentifier(name);
    return this.createTable(tableIdentifier, schema, partitionSpec, null);
  }

  public Table createTable(
      final String name,
      final Schema schema,
      final PartitionSpec partitionSpec,
      final Map<String, String> properties) {
    final TableIdentifier tableIdentifier = this.tableIdentifier(name);
    return this.createTable(tableIdentifier, schema, partitionSpec, properties);
  }

  public List<TableIdentifier> listTables(final Namespace namespace) {
    return this.catalog.listTables(namespace);
  }

  public List<TableIdentifier> listTables(final String... levels) {
    return this.catalog.listTables(Namespace.of(levels));
  }

  public boolean dropTable(final TableIdentifier tableIdentifier) {
    return this.dropTable(tableIdentifier, true);
  }

  public boolean dropTable(final TableIdentifier tableIdentifier, final boolean purge) {
    return this.catalog.dropTable(tableIdentifier, purge);
  }

  public boolean dropTable(final String name) {
    return this.dropTable(name, true);
  }

  public boolean dropTable(final String name, final boolean purge) {
    final TableIdentifier tableIdentifier = this.tableIdentifier(name);
    return this.catalog.dropTable(tableIdentifier, purge);
  }

  public void renameTable(final String fromName, final String toName) {
    this.renameTable(this.tableIdentifier(fromName), this.tableIdentifier(toName));
  }

  public void renameTable(final TableIdentifier from, final TableIdentifier to) {
    this.catalog.renameTable(from, to);
  }
}
