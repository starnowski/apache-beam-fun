package com.github.starnowski.apache.beam.fun;

import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.*;
import com.google.cloud.firestore.FirestoreOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.auth.NoopCredentialFactory;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.jboss.byteman.contrib.bmunit.WithByteman;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
import org.junit.Before;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BigQueryEmulatorContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@WithByteman
public class CountTest {

    // Our static input data, which will make up the initial PCollection.
    static final String[] WORDS_ARRAY = new String[] {
            "hi", "there", "hi", "hi", "sue", "bob",
            "hi", "sue", "", "", "ZOW", "bob", ""};

    static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

    private static Network sharedNetwork = Network.newNetwork();
    private static GenericContainer<?> csContainer = new GenericContainer<>("fsouza/fake-gcs-server")
            .withNetwork(sharedNetwork)
//            .withEnv("GCS_BUCKETS", "test-bucket")
            .withExposedPorts(4443);
    private static BigQueryEmulatorContainer bigQueryContainer = new BigQueryEmulatorContainer("ghcr.io/goccy/bigquery-emulator:0.4.3")
            .withNetwork(sharedNetwork);

    static BigQuery bigQuery;

    @BeforeAll
    public static void setupDataLake() {
        csContainer.start();
        System.out.println("host: " + csContainer.getHost() + ":" + csContainer.getMappedPort(4443));
        bigQueryContainer
                .withEnv("STORAGE_EMULATOR_HOST", csContainer.getHost() + ":" + csContainer.getMappedPort(4443))
                .start();
//        clearDataSet();
        bigQuery = com.google.cloud.bigquery.BigQueryOptions.newBuilder()
                .setHost(bigQueryContainer.getEmulatorHttpEndpoint())
                .setCredentials(NoCredentials.getInstance())
                .setProjectId(bigQueryContainer.getProjectId())
                .build().getService();
        createDataSet();
        createTable();
        createTableRecords();
    }

    private static void createTableRecords() {
        var tableId = TableId.of("samples", "weather_stations");
        InsertAllRequest insertAllRequest = InsertAllRequest.newBuilder(tableId)
                .addRow(InsertAllRequest.RowToInsert.of("x", Map.of("column_01", "Val1", "column_02", "val2"))).build();

        bigQuery.insertAll(insertAllRequest);
    }

    @AfterAll
    public static void tearDown() {
        bigQueryContainer.stop();
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
                Field.of("column_01", StandardSQLTypeName.STRING),
                Field.of("column_02", StandardSQLTypeName.STRING));
        var tableId = TableId.of("samples", "weather_stations");
        var tableDefinition = StandardTableDefinition.newBuilder().setSchema(schema)
//                .setNumBytes(Long.valueOf(0L))
                .setNumBytes(Long.valueOf(1L))
//                .setNumBytes(Long.valueOf(Integer.MAX_VALUE))
                .build();
        var tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

        bigQuery.create(tableInfo);
    }


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

    private static final String WEATHER_SAMPLES_TABLE =
            "test-project.samples.weather_stations";
    private static final String WEATHER_SAMPLES_SUMMARY_TABLE =
            "test-project.samples.weather_summary";

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

}