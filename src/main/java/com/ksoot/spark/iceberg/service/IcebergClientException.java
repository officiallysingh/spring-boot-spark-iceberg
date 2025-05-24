package com.ksoot.spark.iceberg.service;

import org.springframework.core.NestedRuntimeException;

public class IcebergClientException extends NestedRuntimeException {

  public IcebergClientException(final String message) {
    super(message);
  }

  public IcebergClientException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
