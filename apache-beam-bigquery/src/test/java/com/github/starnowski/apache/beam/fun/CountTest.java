package com.github.starnowski.apache.beam.fun;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.auth.NoopCredentialFactory;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.jboss.byteman.contrib.bmunit.WithByteman;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BigQueryEmulatorContainer;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@WithByteman
public class CountTest {

    // Our static input data, which will make up the initial PCollection.
    static final String[] WORDS_ARRAY = new String[]{
            "hi", "there", "hi", "hi", "sue", "bob",
            "hi", "sue", "", "", "ZOW", "bob", ""};
    static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);
    private static final Logger LOGGER = LoggerFactory.getLogger(CountTest.class);
    private static final String WEATHER_SAMPLES_TABLE =
            "test-project.samples.weather_stations";
    private static final String WEATHER_SAMPLES_SUMMARY_TABLE =
            "test-project.samples.weather_summary";
    static BigQuery bigQuery;
    private static final Network sharedNetwork = Network.newNetwork();
    private static final GenericContainer<?> csContainer = new GenericContainer<>("fsouza/fake-gcs-server")
            .withNetwork(sharedNetwork)
            .withEnv("GCS_BUCKETS", "test-bucket")
            .withNetworkAliases("fakegcs.com")
            .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint(
                    "/bin/fake-gcs-server",
                    "-log-level", "debug",
//                    "-external-url", "fakegcs:4443",
                    "-scheme", "http"
//                    "-scheme", "http"
            ))
            .withExposedPorts(4443);
    private static final BigQueryEmulatorContainer bigQueryContainer = new BigQueryEmulatorContainer("ghcr.io/goccy/bigquery-emulator:0.4.3")
            .withCommand(new String[]{"--project", "test-project",
                    "--log-level", "debug",
                    "--dataset", "samples"})
            .withNetwork(sharedNetwork);

    @BeforeAll
    public static void setupDataLake() throws IOException, InterruptedException {
//        Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(LOGGER);
//        csContainer.setLogConsumers(Arrays.asList(logConsumer));
        csContainer.setLogConsumers(Arrays.asList(outputFrame -> System.out.println("[Cloud storage]" + outputFrame.getUtf8String())));
//        bigQueryContainer.setLogConsumers(Arrays.asList(logConsumer));
//        bigQueryContainer.withLogConsumer(outputFrame -> System.out.println(outputFrame.getUtf8String()));
        bigQueryContainer.setLogConsumers(Arrays.asList(outputFrame -> System.out.println("[BigQuery]" + outputFrame.getUtf8String())));
        csContainer.start();
        System.out.println("host: " + csContainer.getHost() + ":" + csContainer.getMappedPort(4443));
        bigQueryContainer
//                .withEnv("STORAGE_EMULATOR_HOST", "fakegcs:4443")
//                .withEnv("STORAGE_EMULATOR_HOST", "https://fakegcs:" + csContainer.getMappedPort(4443))
//                .withEnv("STORAGE_EMULATOR_HOST", "https://fakegcs:4443")
//                .withEnv("STORAGE_EMULATOR_HOST", "http://fakegcs:4443")
                .withEnv("STORAGE_EMULATOR_HOST", "http://fakegcs.com:4443")
//                .withEnv("STORAGE_EMULATOR_HOST", "https://0.0.0.0:4443")
//                    .withEnv("STORAGE_EMULATOR_HOST", "http://fakegcs:" + csContainer.getMappedPort(4443))
//                    .withEnv("STORAGE_EMULATOR_HOST", "http://fakegcs:" + csContainer.getMappedPort(4443))
//                .withEnv("STORAGE_EMULATOR_HOST", "https://127.0.0.1:4443")
//                .withEnv("STORAGE_EMULATOR_HOST", "https://172.17.0.2:4443")
//                .withEnv("STORAGE_EMULATOR_HOST", "https://127.0.0.1:" + csContainer.getMappedPort(4443))
                .start();

        System.out.println("bigquery host: " + bigQueryContainer.getHost() + " ports: " + bigQueryContainer.getExposedPorts().stream().map(p -> p + "_" + bigQueryContainer.getMappedPort(p)).collect(Collectors.joining(",")));

        bigQuery = com.google.cloud.bigquery.BigQueryOptions.newBuilder()
                .setHost(bigQueryContainer.getEmulatorHttpEndpoint())
                .setCredentials(NoCredentials.getInstance())
                .setProjectId(bigQueryContainer.getProjectId())
                .build().getService();
//        createDataSet();
        createTable();
        createTableRecords();

        //Test DNS
//        Container.ExecResult commandResult = bigQueryContainer.execInContainer("nslookup fakegcs");
//        Container.ExecResult commandResult = bigQueryContainer.execInContainer("dig fakegcs");
        //apt-get update && apt-get install -y passw
//        executeCommandOnBiqQueryContainer("apt-get update && apt-get install -y passwd");
//        executeCommandOnBiqQueryContainer("yum install -y glibc-common");
//        executeCommandOnBiqQueryContainer("getent fakegcs");
    }

    private static void executeCommandOnBiqQueryContainer(String command) throws IOException, InterruptedException {
        Container.ExecResult commandResult = bigQueryContainer.execInContainer(command);
        System.out.println("Command stdout: " + commandResult.getStdout());
        System.out.println("Command stderr: " + commandResult.getStderr());
        System.out.println("Exit code: " + commandResult.getExitCode());
    }

    private static void createTableRecords() {
        var tableId = TableId.of("samples", "weather_stations");
        InsertAllRequest insertAllRequest = InsertAllRequest.newBuilder(tableId)
                .addRow(InsertAllRequest.RowToInsert.of("x", Map.of("month", 2, "tornado_count", 15))).build();

        bigQuery.insertAll(insertAllRequest);
    }

    @AfterAll
    public static void tearDown() {
        bigQueryContainer.stop();
        System.out.println("BigQuery logs: " + bigQueryContainer.getLogs());
        csContainer.stop();
        System.out.println("Cloud storage logs: " + csContainer.getLogs());
    }

    public static void createDataSet() {
        BigQuery bigQuery = com.google.cloud.bigquery.BigQueryOptions.newBuilder()
                .setHost(bigQueryContainer.getEmulatorHttpEndpoint())
                .setCredentials(NoCredentials.getInstance())
                .setProjectId(bigQueryContainer.getProjectId())
                .build().getService();
        var datasetInfo = DatasetInfo.newBuilder("samples").build();
        bigQuery.create(datasetInfo);
    }

    public static void createTable() {
        Schema schema = Schema.of(
                Field.of("month", StandardSQLTypeName.INT64),
                Field.of("tornado_count", StandardSQLTypeName.INT64));
        var tableId = TableId.of("samples", "weather_stations");
        var tableDefinition = StandardTableDefinition.newBuilder().setSchema(schema)
//                .setNumBytes(Long.valueOf(0L))
                .setNumBytes(Long.valueOf(1L))
//                .setNumBytes(Long.valueOf(Integer.MAX_VALUE))
                .build();
        var tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

        bigQuery.create(tableInfo);
    }

    @Timeout(unit = TimeUnit.MINUTES, value = 2, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    @Test
    @BMUnitConfig(verbose = true, bmunitVerbose = true)
    @BMRules(rules = {
            @BMRule(name = "BigQueryOptions getBigQueryEndpoint",
                    targetClass = "org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions",
                    targetMethod = "getBigQueryEndpoint",
                    isInterface = true,
                    targetLocation = "AT ENTRY",
                    helper = "com.github.starnowski.apache.beam.fun.BMUnitHelperWithStaticStringProperty",
                    action = "RETURN getStaticStringProperty()")
            ,
            @BMRule(name = "BigQueryOptions getProject",
                    targetClass = "org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions",
                    targetMethod = "getProject",
                    isInterface = true,
                    targetLocation = "AT ENTRY",
                    helper = "com.github.starnowski.apache.beam.fun.BMUnitHelperWithStaticStringProperty",
                    action = "RETURN \"test-project\"")
            ,
            @BMRule(name = "BigQueryOptions getBigQueryProject",
                    targetClass = "org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions",
                    targetMethod = "getBigQueryProject",
                    isInterface = true,
                    targetLocation = "AT ENTRY",
                    helper = "com.github.starnowski.apache.beam.fun.BMUnitHelperWithStaticStringProperty",
                    action = "RETURN \"test-project\"")
    })
    public void testCount() {
        // Create a test pipeline.
        BigQueryOptions options = PipelineOptionsFactory.as(BigQueryOptions.class);
        options.setBigQueryEndpoint(bigQueryContainer.getEmulatorHttpEndpoint());
        options.setProject(bigQueryContainer.getProjectId());
        options.setBigQueryProject(bigQueryContainer.getProjectId());
        options.setTempLocation("gs://test-project/test-bucket");
        options.setCredentialFactoryClass(NoopCredentialFactory.class);
        BMUnitHelperWithStaticStringProperty.setStaticStringProperty(bigQueryContainer.getEmulatorHttpEndpoint());
        Pipeline p = Pipeline.create(options);


        Options subOptions = PipelineOptionsFactory.create().as(Options.class);
        applyBigQueryTornadoes(p, subOptions);
        p.run().waitUntilFinish();
    }

    private void applyBigQueryTornadoes(Pipeline p, Options options) {
        // Build the table schema for the output table.
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("month").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("tornado_count").setType("INTEGER"));
        TableSchema schema = new TableSchema().setFields(fields);

        BigQueryIO.TypedRead<TableRow> bigqueryIO;
        if (!options.getInputQuery().isEmpty()) {
            bigqueryIO =
                    BigQueryIO.readTableRows()
                            .fromQuery(options.getInputQuery())
                            .usingStandardSql()
                            .withMethod(options.getReadMethod());
        } else {
            bigqueryIO =
                    BigQueryIO.readTableRows().from(options.getInput()).withMethod(options.getReadMethod());

            // Selected fields only applies when using Method.DIRECT_READ and
            // when reading directly from a table.
            if (options.getReadMethod() == BigQueryIO.TypedRead.Method.DIRECT_READ) {
                bigqueryIO = bigqueryIO.withSelectedFields(Arrays.asList("month", "tornado"));
            }
        }

        PCollection<TableRow> rowsFromBigQuery = p.apply(bigqueryIO);

        rowsFromBigQuery
                .apply(new CountTornadoes())
                .apply(
                        BigQueryIO.writeTableRows()
                                .to(options.getOutput())
                                .withSchema(schema)
                                .withCreateDisposition(options.getCreateDisposition())
                                .withWriteDisposition(options.getWriteDisposition())
                                .withMethod(options.getWriteMethod()));
    }

    public interface Options extends PipelineOptions {
        @Description("Table to read from, specified as <project_id>:<dataset_id>.<table_id>")
        @Default.String(WEATHER_SAMPLES_TABLE)
        String getInput();

        void setInput(String value);

        @Description("SQL Query to read from, will be used if Input is not set.")
        @Default.String("")
        String getInputQuery();

        void setInputQuery(String value);

        @Description("Read method to use to read from BigQuery")
        @Default.Enum("EXPORT")
        BigQueryIO.TypedRead.Method getReadMethod();

        void setReadMethod(BigQueryIO.TypedRead.Method value);

        @Description("Write method to use to write to BigQuery")
        @Default.Enum("DEFAULT")
        BigQueryIO.Write.Method getWriteMethod();

        void setWriteMethod(BigQueryIO.Write.Method value);

        @Description("Write disposition to use to write to BigQuery")
        @Default.Enum("WRITE_TRUNCATE")
        BigQueryIO.Write.WriteDisposition getWriteDisposition();

        void setWriteDisposition(BigQueryIO.Write.WriteDisposition value);

        @Description("Create disposition to use to write to BigQuery")
        @Default.Enum("CREATE_IF_NEEDED")
        BigQueryIO.Write.CreateDisposition getCreateDisposition();

        void setCreateDisposition(BigQueryIO.Write.CreateDisposition value);

        @Description(
                "BigQuery table to write to, specified as "
                        + "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
        @Validation.Required
        @Default.String(WEATHER_SAMPLES_SUMMARY_TABLE)
        String getOutput();

        void setOutput(String value);
    }

    /**
     * Examines each row in the input table. If a tornado was recorded in that sample, the month in
     * which it occurred is output.
     */
    static class ExtractTornadoesFn extends DoFn<TableRow, Integer> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow row = c.element();
            if ((Boolean) row.get("tornado")) {
                c.output(Integer.parseInt((String) row.get("month")));
            }
        }
    }

    /**
     * Prepares the data for writing to BigQuery by building a TableRow object containing an integer
     * representation of month and the number of tornadoes that occurred in each month.
     */
    static class FormatCountsFn extends DoFn<KV<Integer, Long>, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow row =
                    new TableRow()
                            .set("month", c.element().getKey())
                            .set("tornado_count", c.element().getValue());
            c.output(row);
        }
    }

    static class CountTornadoes extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {
        @Override
        public PCollection<TableRow> expand(PCollection<TableRow> rows) {

            // row... => month...
            PCollection<Integer> tornadoes = rows.apply(ParDo.of(new ExtractTornadoesFn()));

            // month... => <month,count>...
            PCollection<KV<Integer, Long>> tornadoCounts = tornadoes.apply(Count.perElement());

            // <month,count>... => row...
            PCollection<TableRow> results = tornadoCounts.apply(ParDo.of(new FormatCountsFn()));

            return results;
        }
    }

}