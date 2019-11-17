package com.google.demo;

import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.demo.model.Session;
import com.google.demo.model.Session.Status;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class DemoGenerator {

  private static final Logger log = Logger.getLogger(DemoGenerator.class.getName());

  private static final String ORIGINAL_SESSION_SIZE = "original_session_size";
  private static final String INSERTS_PER_BATCH = "inserts_per_batch";
  private static final String PAUSE_BETWEEN_BATCHES = "pause_between_batches";
  private static final String UPDATE_PERCENT = "update_percent";
  private static final String DELETE_PERCENT = "delete_percent";
  private static final String PROJECT_ID = "project_id";

  private static class Parameters {

    int insertsPerBatch;
    int pauseBetweenBatchInSeconds;
    int originalSessionCount;
    int percentOfUpdatesPerBatch;
    int percentOfDeletesPerBatch;
    String projectId;
  }

  public static void main(String[] args) throws InterruptedException, IOException {
    Parameters params = getParameters(args);

    BigQueryService bigQueryService = new BigQueryService(
        BigQueryOptions.getDefaultInstance().getService());

    BigTableService bigTableService = new BigTableService(BigtableDataClient.create(
        BigtableDataSettings.newBuilder().setProjectId(params.projectId)
            .setInstanceId("bq-sync-instance")
            .build()));

    if (params.originalSessionCount > 0) {
      log.info("Starting batch inserts...");
      TableId mainSessionTableId = TableId.of(params.projectId, "cdc_demo", "session_main");
      bigQueryService.doBatchInserts(mainSessionTableId, params.originalSessionCount, 100);
    }

    log.info("Starting data sync simulation...");
    TableId deltaSessionTableId = TableId.of(params.projectId, "cdc_demo", "session_delta");
    doStreamingInserts(bigQueryService, bigTableService, deltaSessionTableId, params);
  }

  private static void doStreamingInserts(BigQueryService bigQueryService,
      BigTableService bigTableService, TableId tableId,
      Parameters parameters)
      throws InterruptedException {

    ArrayList<Session> previousRecords = new ArrayList<>();

    Random random = new Random();

    File stopFile = new File("sync.stop");
    while (true) {
      if (stopFile.exists()) {
        log.info("Found " + stopFile.getName() + " file. Finishing processing.");
        break;
      }

      // Creating containers for BigTable and BigQuery to store mutations and inserts
      BulkMutation bulkMutation = bigTableService.createBulkMutationForSession();
      InsertAllRequest.Builder insertRequestBuilder = InsertAllRequest.newBuilder(tableId);

      // Creating new inserts
      ArrayList<Session> newInserts = new ArrayList<>();
      for (int i = 0; i < parameters.insertsPerBatch; i++) {
        Session session = new Session();
        newInserts.add(session);

        bigTableService.addOrUpdateSession(bulkMutation, session);
        bigQueryService.addInsertRow(insertRequestBuilder, session.toBigQueryRow());
      }

      if (previousRecords.size() > 0) {
        // Simulating updates
        int updateCount = parameters.insertsPerBatch * parameters.percentOfUpdatesPerBatch / 100;
        for (int i = 0; i < updateCount; i++) {
          int nextRecordToUpdate = random.nextInt(previousRecords.size());
          Session session = previousRecords.get(nextRecordToUpdate);
          switch (session.getStatus()) {
            case NEW:
              session.loggedIn("customer" + i);
              break;
            case LOGGED_IN:
              session.logout();
              break;
            default:
              break;
          }

          bigTableService.addOrUpdateSession(bulkMutation, session);
          bigQueryService.addUpdateRow(insertRequestBuilder, session.toBigQueryRow());
        }

        // Simulating deletes
        int deleteCount = parameters.insertsPerBatch * parameters.percentOfDeletesPerBatch / 100;
        for (int i = 0; i < deleteCount; i++) {
          int nextRecordToDelete = random.nextInt(previousRecords.size());

          Session session = previousRecords.get(nextRecordToDelete);
          previousRecords.remove(nextRecordToDelete);

          bigTableService.deleteSession(bulkMutation, session);
          bigQueryService.addDeleteRow(insertRequestBuilder, session.toBigQueryRow());
        }
      }

      // Only after updates and deletes on the previous records are done we are adding the newly created inserts
      previousRecords.addAll(newInserts);

      // Let's keep a limited number of recent records which we keep updating or deleting
      while (previousRecords.size() > 10_000) {
        Session session = previousRecords.remove(0);
        if (session.getStatus() != Status.LOGGED_OUT) {
          session.abandon();
          bigTableService.addOrUpdateSession(bulkMutation, session);
          bigQueryService.addUpdateRow(insertRequestBuilder, session.toBigQueryRow());
        }
      }

      // Save the data into BigTable and BigQuery
      bigTableService.bulkUpdate(bulkMutation);
      bigQueryService.runInsertAll(insertRequestBuilder);

      Thread.sleep(1000 * parameters.pauseBetweenBatchInSeconds);
    }
  }


  private static Parameters getParameters(String[] args) {
    Options commandLineOptions = createCommandLineOptions();

    Parameters result = new Parameters();
    try {
      CommandLine cmd = (new DefaultParser()).parse(commandLineOptions, args);
      result.originalSessionCount = getIntParameter(cmd, ORIGINAL_SESSION_SIZE, 0);
      result.insertsPerBatch = getIntParameter(cmd, INSERTS_PER_BATCH, 500);
      result.pauseBetweenBatchInSeconds = getIntParameter(cmd, PAUSE_BETWEEN_BATCHES, 20);
      result.percentOfDeletesPerBatch = getIntParameter(cmd, DELETE_PERCENT, 5);
      result.percentOfUpdatesPerBatch = getIntParameter(cmd, UPDATE_PERCENT, 20);
      result.projectId = cmd.getOptionValue(PROJECT_ID);

      return result;
    } catch (ParseException e) {
      new HelpFormatter().printHelp("java -jar demogenerator.jar", commandLineOptions);
      e.printStackTrace();
      System.exit(-1);
      return null;
    }
  }

  private static int getIntParameter(CommandLine cmd, String optionName, int defaultValue)
      throws ParseException {
    if (cmd.getOptionValue(optionName) == null) {
      return defaultValue;
    }
    return ((Number) (cmd.getParsedOptionValue(optionName)))
        .intValue();
  }


  private static Options createCommandLineOptions() {
    Options options = new Options();

    options.addOption(
        Option.builder().longOpt(ORIGINAL_SESSION_SIZE)
            .desc("Original number of records in the session table")
            .hasArg()
            .type(Number.class)
            .argName("number").build());

    options.addOption(
        Option.builder().longOpt(INSERTS_PER_BATCH)
            .desc("Number of inserts per batch")
            .hasArg()
            .type(Number.class)
            .argName("number").build());

    options.addOption(
        Option.builder().longOpt(PAUSE_BETWEEN_BATCHES)
            .desc("Pause between batches")
            .hasArg()
            .type(Number.class)
            .argName("seconds").build());

    options.addOption(
        Option.builder().longOpt(UPDATE_PERCENT)
            .desc("Percentage of updates relative to inserts")
            .hasArg()
            .type(Number.class)
            .argName("number").build());

    options.addOption(
        Option.builder().longOpt(DELETE_PERCENT)
            .desc("Percentage of deletes relative to inserts")
            .hasArg()
            .type(Number.class)
            .argName("number").build());

    options.addOption(
        Option.builder().longOpt(PROJECT_ID).required()
            .desc("Project")
            .hasArg()
            .argName("GCP project ID").build());

    return options;
  }
}
