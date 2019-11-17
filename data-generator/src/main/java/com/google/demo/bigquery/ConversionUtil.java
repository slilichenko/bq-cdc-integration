package com.google.demo.bigquery;

import com.google.cloud.bigquery.QueryParameterValue;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class ConversionUtil {

  public static String convertToTimestamp(Instant instant) {
    if (instant == null) {
      return null;
    }
    return QueryParameterValue.timestamp(ChronoUnit.MICROS.between(Instant.EPOCH, instant))
        .getValue();
  }
}
