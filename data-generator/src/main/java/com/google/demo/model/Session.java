package com.google.demo.model;

import com.google.demo.Constants;
import com.google.demo.bigquery.ConversionUtil;
import com.google.demo.bigquery.Struct;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Session {

  public enum Status {NEW, ABANDONED, LOGGED_IN, LOGGED_OUT}

  private String sessionId;
  private Status status;
  private String customerKey;
  private Instant start;
  private Instant end;

  public Session() {
    sessionId = UUID.randomUUID().toString();
    status = Status.NEW;
    start = Instant.now();
  }

  public void loggedIn(String customerKey) {
    status = Status.LOGGED_IN;
    this.customerKey = customerKey;
  }

  public void abandon() {
    status = Status.ABANDONED;
    end = Instant.now();
  }

  public void logout() {
    status = Status.LOGGED_OUT;
    end = Instant.now();
  }

  public Map<String, Object> toBigQueryRow() {
    Map<String, Object> result = new HashMap<>();
    result.put(Constants.SESSION_ID_COLUMN, sessionId);
    result.put(Constants.STATUS_COLUMN, status.name());
    result.put(Constants.CUSTOMER_KEY_COLUMN, customerKey);
    result.put(Constants.START_COLUMN, ConversionUtil.convertToTimestamp(start));
    result.put(Constants.END_COLUMN, ConversionUtil.convertToTimestamp(end));
    return result;
  }

  public Struct toBigQueryStruct() {
    Struct result = new Struct();
    result.addString(Constants.SESSION_ID_COLUMN, sessionId)
        .addString(Constants.STATUS_COLUMN, status.name())
        .addString(Constants.CUSTOMER_KEY_COLUMN, customerKey)
        .addTimestamp(Constants.START_COLUMN, start)
        .addTimestamp(Constants.END_COLUMN, end);
    return result;
  }

  public String getSessionId() {
    return sessionId;
  }

  public Status getStatus() {
    return status;
  }

  public String getCustomerKey() {
    return customerKey;
  }

  public Instant getStart() {
    return start;
  }

  public Instant getEnd() {
    return end;
  }
}
