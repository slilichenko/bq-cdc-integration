package com.google.demo;

import static com.google.demo.Constants.CUSTOMER_KEY_COLUMN;
import static com.google.demo.Constants.END_COLUMN;
import static com.google.demo.Constants.START_COLUMN;
import static com.google.demo.Constants.STATUS_COLUMN;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.demo.bigquery.ConversionUtil;
import com.google.demo.model.Session;

public class BigTableService {

  private BigtableDataClient bigtableClient;

  public BigTableService(BigtableDataClient bigtableDataClient) {
    this.bigtableClient = bigtableDataClient;
  }

  public BulkMutation createBulkMutationForSession() {
    return BulkMutation.create("session");
  }

  public void addOrUpdateSession(BulkMutation bulkMutation, Session session) {
    Mutation mutation = Mutation.create();
    setNonNullCell(mutation, "main", STATUS_COLUMN, session.getStatus().name());
    setNonNullCell(mutation, "main", CUSTOMER_KEY_COLUMN, session.getCustomerKey());
    setNonNullCell(mutation, "main", START_COLUMN,
        ConversionUtil.convertToTimestamp(session.getStart()));
    setNonNullCell(mutation, "main", END_COLUMN,
        ConversionUtil.convertToTimestamp(session.getEnd()));

    bulkMutation.add(session.getSessionId(), mutation);
  }

  private static Mutation setNonNullCell(Mutation mutation, String familyName,
      String columnName, String value) {
    if (value == null) {
      return mutation;
    }
    return mutation.setCell(familyName, columnName, value);
  }

  public void deleteSession(BulkMutation bulkMutation, Session session) {
    bulkMutation.add(session.getSessionId(), Mutation.create().deleteRow());
  }

  public void bulkUpdate(BulkMutation bulkMutation) {
    bigtableClient.bulkMutateRows(bulkMutation);
  }
}
