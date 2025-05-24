package com.ksoot.spark.iceberg.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.annotation.Nullable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.builder.Builder;
import org.apache.iceberg.types.Type;

@Getter
@ToString
@EqualsAndHashCode(of = "name")
@Valid
public class IcebergField {

  @Schema(description = "Field name", example = "conv_rate")
  @NotEmpty
  private final String name;

  @Schema(description = "Field value Type")
  @NotNull
  private final Type type;

  @Schema(description = "Field mandatory or not", example = "true")
  private final boolean required;

  @Schema(description = "Field description", example = "Field for testing")
  private final String description;

  @JsonIgnore
  public boolean isOptional() {
    return !this.required;
  }

  private IcebergField(
      final String name, final Type type, final boolean required, final String description) {
    this.name = name;
    this.type = type;
    this.required = required;
    this.description = description;
  }

  @JsonCreator
  public static IcebergField of(
      @JsonProperty("name") final String name,
      @JsonProperty("type") final Type type,
      @JsonProperty("required") final boolean required,
      @JsonProperty("description") final String description) {
    return new IcebergField(name, type, required, description);
  }

  public static IcebergField required(
      final String name, final Type type, final String description) {
    return new IcebergField(name, type, true, description);
  }

  public static IcebergField required(final String name, final Type type) {
    return new IcebergField(name, type, true, null);
  }

  public static IcebergField optional(
      final String name, final Type type, final String description) {
    return new IcebergField(name, type, false, description);
  }

  public static IcebergField optional(final String name, final Type type) {
    return new IcebergField(name, type, false, null);
  }

  // Asking only for required arguments
  public static IcebergField of(final String name, final Type type) {
    return IcebergField.of(name, type, false, null);
  }

  public static TypeBuilder name(final String name) {
    return new FeatureFieldBuilder(name);
  }

  public interface TypeBuilder {

    RequiredBuilder type(final Type type);
  }

  public interface RequiredBuilder extends DescriptionBuilder {

    DescriptionBuilder required();

    DescriptionBuilder optional();

    DescriptionBuilder required(final boolean required);
  }

  public interface DescriptionBuilder extends Builder<IcebergField> {

    Builder<IcebergField> description(final String description);
  }

  public static class FeatureFieldBuilder
      implements TypeBuilder, RequiredBuilder, DescriptionBuilder {

    private final String name;

    private Type type;

    private boolean required = false;

    private String description;

    FeatureFieldBuilder(final String name) {
      this.name = name;
    }

    @Override
    public RequiredBuilder type(final Type type) {
      this.type = type;
      return this;
    }

    @Override
    public DescriptionBuilder required() {
      this.required = true;
      return this;
    }

    @Override
    public DescriptionBuilder optional() {
      this.required = false;
      return this;
    }

    @Override
    public DescriptionBuilder required(boolean required) {
      this.required = required;
      return this;
    }

    @Override
    public Builder<IcebergField> description(@Nullable final String description) {
      this.description = description;
      return this;
    }

    @Override
    public IcebergField build() {
      return IcebergField.of(this.name, this.type, this.required, this.description);
    }
  }
}
