package com.sbytestream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;
import software.amazon.awssdk.services.athena.paginators.GetQueryResultsIterable;
import java.time.Instant;
import java.util.List;

@SpringBootApplication
public class App implements CommandLineRunner {
    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }

    @Override
    public void run(String[] args) throws Exception {
        if (args.length == 0) {
            help();
            return;
        }

        cmd = new CmdLine(args);
        String awsRegion = cmd.getFlagValue(FLAG_AWS_REGION);
        if (isNullOrEmpty(awsRegion, "Region")) return;

        Region region = Region.of(awsRegion);

        String bucketName = cmd.getFlagValue(FLAG_BUCKET_NAME);
        if (isNullOrEmpty(bucketName, "Output-bucket")) return;

        String database = cmd.getFlagValue(FLAG_DATABASE);
        if (isNullOrEmpty(database, "Database")) return;

        if (cmd.isFlagPresent(FLAG_QUERY) && cmd.isFlagPresent(FLAG_SCRIPT)) {
            System.out.println("Use either -q or -s.");
            return;
        }

        String query = cmd.getFlagValue(FLAG_QUERY);
        if (cmd.isFlagPresent(FLAG_QUERY)) {
            if (isNullOrEmpty(query, "Query")) return;
            logger.info(String.format("SQL: %s", query));
        }

        String script = cmd.getFlagValue(FLAG_SCRIPT);
        if (cmd.isFlagPresent(FLAG_SCRIPT)) {
            if (isNullOrEmpty(script, "Script")) return;
            logger.info(String.format("SQL script: %s", script));
        }

        try {
            if (cmd.isFlagPresent(FLAG_QUERY)) {
                runQuery(region, bucketName, database, query);
            }
            else {
                runScript(region, bucketName, database, script);
            }
        }
        catch(Exception e) {
            logger.error("An exception occurred while running query", e);
            System.out.println(String.format("Something bad happened: %s. See log file in logs\\ folder for details.",
                    e.getMessage()));
        }
    }

    private void help() {
        System.out.println("Runs Athena an query specified as argument or multiple Athena queries stored in a text file.");
        System.out.println("Syntax: java -jar aetl-1.0.jar  -r <aws-region-name> -b <output-s3-bucket> [-q \"<athena-query>\"] [-qf \"<query-file>\"");
        System.out.println("Example:");
        System.out.println("Execute a single query:");
        System.out.println("java -jar aetl-1.0.jar -r us-east-1 -b etl-for-surveilx-poc/output/ -d mydatabase -q \"select * from trade\"");
        System.out.println("Execute a script containing multiple queries, queries must be delimited by the word 'go':");
        System.out.println("java -jar aetl-1.0.jar  -r us-east-1 -b my-s3-bucket/my-output-folder/ -d mydatabase -s \"c:\\temp\\athena.sql\"");
        System.out.println("Web: https://sbytestream.pythonanywhere.com");
    }

    private static boolean isNullOrEmpty(String value, String valueName) {
        if (value == null || value == "") {
            System.out.printf("%s has not been specified.", valueName);
            return true;
        }
        else {
            return false;
        }
    }

    private void runScript(Region region, String outputBucket, String database, String script) throws Exception {
        AthenaClient client = AthenaClient.builder().region(region).build();
        Stopwatch stopwatch = new Stopwatch();

        SqlScript sqlScript = new SqlScript(script, new SqlScript.ISqlScriptClient() {
            @Override
            public void statementFound(String sql) {
                try {
                    System.out.printf("Executing: %s\n", sql);
                    stopwatch.start();

                    StartQueryExecutionResponse response = runAthenaQuery(client, outputBucket, database, sql);
                    String executionId = response.queryExecutionId();

                    System.out.println("Waiting for the query to complete...");
                    waitForAthenaResults(client, executionId);
                    Instant endedAt = Instant.now();
                    System.out.printf("Query took %d ms to complete.\n", stopwatch.stop());

                    displayAthenaResults(client, executionId);
                }
                catch(Exception e) {
                    logger.error("An exception occurred while running query", e);
                    System.out.printf("Error running query: %s", e.getMessage());
                }
            }
        });

        sqlScript.parse();
    }

    private void runQuery(Region region, String outputBucket, String database, String sql) throws Exception {
        AthenaClient client = AthenaClient.builder().region(region).build();
        Stopwatch stopwatch = new Stopwatch();

        stopwatch.start();
        StartQueryExecutionResponse response = runAthenaQuery(client, outputBucket, database, sql);
        String executionId = response.queryExecutionId();

        System.out.println("Waiting for the query to complete...");
        waitForAthenaResults(client, executionId);
        Instant endedAt = Instant.now();
        System.out.printf("Query took %d ms to complete.\n", stopwatch.stop());

        displayAthenaResults(client, executionId);
    }

    private StartQueryExecutionResponse runAthenaQuery(AthenaClient client, String outputBucket, String database, String sql) throws Exception {
        QueryExecutionContext queryExecutionContext = QueryExecutionContext.builder()
                .database(database)
                .build();

        String outputBucketUrn = String.format("s3://%s", outputBucket);
        System.out.printf("Query output will be stored in %s\n", outputBucketUrn);
        ResultConfiguration resultConfiguration = ResultConfiguration.builder()
                .outputLocation(outputBucketUrn)
                .build();

        StartQueryExecutionRequest startQueryExecutionRequest = StartQueryExecutionRequest.builder()
                .queryString(sql)
                .queryExecutionContext(queryExecutionContext)
                .resultConfiguration(resultConfiguration)
                .build();

        StartQueryExecutionResponse response = client.startQueryExecution(startQueryExecutionRequest);
        return response;
    }

    private void waitForAthenaResults(AthenaClient client, String executionId) throws InterruptedException {
        GetQueryExecutionRequest request = GetQueryExecutionRequest.builder().queryExecutionId(executionId).build();
        boolean executionCompleted = false;

        while(!executionCompleted) {
            GetQueryExecutionResponse response = client.getQueryExecution(request);
            QueryExecutionState state = response.queryExecution().status().state();

            switch(state) {
                case SUCCEEDED:
                    executionCompleted = true;
                    break;

                case CANCELLED:
                    throw new RuntimeException("The Athena query was cancelled");

                case FAILED:
                    throw new RuntimeException(String.format("The Athena query failed to run with error message: %s",
                            response.queryExecution().status().stateChangeReason()));
            }

            Thread.sleep(ATHENA_POLL_INTERVAL);
        }
    }

    private void displayAthenaResults(AthenaClient client, String executionId) {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();

        GetQueryResultsRequest request = GetQueryResultsRequest.builder()
                .queryExecutionId(executionId)
                .build();

        GetQueryResultsIterable resultsIterable = client.getQueryResultsPaginator(request);
        int count = 0;

        for (GetQueryResultsResponse result : resultsIterable) {
            List<ColumnInfo> columnInfoList = result.resultSet().resultSetMetadata().columnInfo();
            List<Row> results = result.resultSet().rows();
            displayColumnInfo(columnInfoList);
            count += displayRows(results);
        }

        System.out.printf("\nRetrieved %d rows in %d ms.\n", count, stopwatch.stop());
    }

    private void displayColumnInfo(List<ColumnInfo> columns) {
        for(int n=0; n < columns.size(); n++) {
            ColumnInfo column = columns.get(n);
            if (n != columns.size() - 1) {
                System.out.printf("%s,", column.name());
            }
            else {
                System.out.printf("%s\n", column.name());
            }
        }
    }

    private int displayRows(List<Row> rows) {
        int n;
        for(n=1; n < rows.size(); n++) {
            Row row = rows.get(n);
            List<Datum> data = row.data();
            for(Datum datum : data) {
                System.out.printf("%s,", datum.varCharValue());
            }
            System.out.printf("\n");
        }
        return n;
    }

    private static CmdLine cmd;
    private static final Logger logger = LoggerFactory.getLogger(App.class);
    private final String FLAG_AWS_REGION = "r";
    private final String FLAG_BUCKET_NAME = "b";
    private final String FLAG_DATABASE = "d";
    private final String FLAG_QUERY = "q";
    private final String FLAG_SCRIPT = "s";
    private final int ATHENA_POLL_INTERVAL = 500;
}
