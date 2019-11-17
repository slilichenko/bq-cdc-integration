package com.google.demo;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllRequest.Builder;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.demo.bigquery.Struct;
import com.google.demo.model.Session;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class BigQueryService {
  private static final Logger log = Logger.getLogger(BigQueryService.class.getName());

  public static final String DI_SEQUENCE_COLUMN = "di_sequence_number";
  public static final String DI_OPERATION_COLUMN = "di_operation_type";

  private BigQuery bigQuery;
  private AtomicInteger insertSequence = new AtomicInteger();

  public BigQueryService(BigQuery bigQuery) {
    this.bigQuery = bigQuery;
  }

  public void addInsertRow(InsertAllRequest.Builder requestBuilder, Map<String,Object> row) {
    addRowWithOperation(requestBuilder, row, "I");
  }

  public void addUpdateRow(InsertAllRequest.Builder requestBuilder, Map<String,Object> row) {
    addRowWithOperation(requestBuilder, row, "U");
  }

  public void addDeleteRow(InsertAllRequest.Builder requestBuilder, Map<String,Object> row) {
    addRowWithOperation(requestBuilder, row, "D");
  }

  private void addRowWithOperation(Builder requestBuilder, Map<String, Object> row, String operation) {
    row.put(DI_OPERATION_COLUMN, operation);
    row.put(DI_SEQUENCE_COLUMN, insertSequence.incrementAndGet());

    requestBuilder.addRow(row);
  }


  public void doBatchInserts(TableId tableId,
      int maxInserts, int batchSize) throws InterruptedException {

    while (maxInserts > 0) {
      int nextBatchSize = Math.min(batchSize, maxInserts);
      log.info("Inserting next batch of " + nextBatchSize + " records.");

      StringBuilder queryBuilder = new StringBuilder();
      queryBuilder.append(
          "INSERT INTO `" +
              tableId.getProject() + '.' + tableId.getDataset() + "." + tableId.getTable()
              + "` SELECT * FROM UNNEST([");
      for (int i = 0; i < nextBatchSize; i++) {
        Session session = new Session();
        Struct struct = session.toBigQueryStruct();

        if (i > 0) {
          queryBuilder.append(",");
        }
        queryBuilder.append(struct.toStructConstant());
      }
      queryBuilder.append("])");

      String query = queryBuilder.toString();
      log.fine("Query: " + query);
      QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query)
          .build();
      TableResult result = bigQuery.query(queryConfig);

      maxInserts -= batchSize;
    }
  }

  public void runInsertAll(Builder insertRequestBuilder) {
    InsertAllRequest insertRequest = insertRequestBuilder.build();
    InsertAllResponse insertResponse = bigQuery.insertAll(insertRequest);
    // TODO: shall we fail immediately?
    if (insertResponse.hasErrors()) {
      log.warning("Errors occurred while inserting rows: " + insertResponse.getInsertErrors());
    } else {
      log.info("Inserted next batch of " + insertRequest.getRows().size() + " rows.");
    }
  }
}
