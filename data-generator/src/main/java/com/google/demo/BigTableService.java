/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
