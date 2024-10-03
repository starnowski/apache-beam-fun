package com.github.starnowski.apache.beam.fun;

import com.google.api.services.bigquery.model.*;
import com.google.cloud.bigquery.StandardSQLTypeName;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.auth.NoopCredentialFactory;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.jboss.byteman.contrib.bmunit.WithByteman;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CountMockedTest {

    // Our static input data, which will make up the initial PCollection.
    static final String[] WORDS_ARRAY = new String[]{
            "hi", "there", "hi", "hi", "sue", "bob",
            "hi", "sue", "", "", "ZOW", "bob", ""};
    static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);
    private static final Logger LOGGER = LoggerFactory.getLogger(CountMockedTest.class);
    private static final String PROJECT_ID = "test-project";
    private static final String DATASET = "samples";
    private static final String WEATHER_SAMPLES_TABLE_ID = "weather_stations";
    private static final String WEATHER_SAMPLES_TABLE =
            PROJECT_ID + ":" + DATASET + "." + WEATHER_SAMPLES_TABLE_ID;
    private static final String WEATHER_SAMPLES_SUMMARY_TABLE =
            PROJECT_ID + ":" + DATASET + "." + "weather_summary";
    private FakeBigQueryServices testServices;

    private static void createTableRecords() {
//        var tableId = TableId.of("samples", "weather_stations");
//        InsertAllRequest insertAllRequest = InsertAllRequest.newBuilder(tableId)
//                .addRow(InsertAllRequest.RowToInsert.of("x", Map.of("month", 2, "tornado_count", 15))).build();

//        bigQuery.insertAll(insertAllRequest);
    }


    public static void createDataSet() {
//        BigQuery bigQuery = com.google.cloud.bigquery.BigQueryOptions.newBuilder()
//                .setHost(bigQueryContainer.getEmulatorHttpEndpoint())
//                .setCredentials(NoCredentials.getInstance())
//                .setProjectId(bigQueryContainer.getProjectId())
//                .build().getService();
//        var datasetInfo = DatasetInfo.newBuilder("samples").build();
//        bigQuery.create(datasetInfo);
    }

    public void createTable(FakeDatasetService datasetService) throws IOException {
//        Schema schema = Schema.of(
//                Field.of("month", StandardSQLTypeName.INT64),
//                Field.of("tornado_count", StandardSQLTypeName.INT64));
//        var tableId = TableId.of(DATASET, "weather_stations");
//        var tableDefinition = StandardTableDefinition.newBuilder().setSchema(schema)
//                .setNumBytes(Long.valueOf(256L))
//                .build();
//        var tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        Table table = new Table();
        TableReference tableReference = new TableReference()
                .setTableId(WEATHER_SAMPLES_TABLE_ID)
                .setProjectId(PROJECT_ID)
                .setDatasetId(DATASET);
        table.setTableReference(tableReference);
        table.setNumBytes(Long.valueOf(256L));
        datasetService.createTable(table);
        datasetService.updateTableSchema(tableReference, new TableSchema()
                .set("month", StandardSQLTypeName.INT64)
                .set("tornado_count", StandardSQLTypeName.INT64));
    }

    @BeforeEach
    public void beforeTest() throws IOException, InterruptedException {
        FakeDatasetService.setUp();
        FakeJobService.setUp();
        FakeJobService fakeJobService = new FakeJobService();
        FakeDatasetService datasetService = new FakeDatasetService();
        testServices = new FakeBigQueryServices()
                .withDatasetService(datasetService)
                .withJobService(fakeJobService);

        datasetService.createDataset(PROJECT_ID, DATASET, null, null, null);
        createTable(datasetService);
        fakeJobService.setNumFailuresExpected(0);
    }

    @Test
    public void testCount() {
        // Create a test pipeline.
        BigQueryOptions options = PipelineOptionsFactory.as(BigQueryOptions.class);
        options.setTempLocation("gs://test-project/test-bucket");
        options.setProject(PROJECT_ID);
        options.setBigQueryProject(PROJECT_ID);
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
                            .withTestServices(new FakeBigQueryServices())
                            .fromQuery(options.getInputQuery())
                            .usingStandardSql()
                            .withMethod(options.getReadMethod())
                            .withTestServices(testServices);
        } else {
            bigqueryIO =
                    BigQueryIO.readTableRows()
                            .withTestServices(testServices)
                            .from(options.getInput()).withMethod(options.getReadMethod());

            // Selected fields only applies when using Method.DIRECT_READ and
            // when reading directly from a table.
            if (options.getReadMethod() == BigQueryIO.TypedRead.Method.DIRECT_READ) {
                bigqueryIO = bigqueryIO.withSelectedFields(Arrays.asList("month", "tornado_count"))
                        .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ)
                ;
            }
        }

        PCollection<TableRow> rowsFromBigQuery = p.apply(bigqueryIO);

        rowsFromBigQuery
                .apply(new CountTornadoes())
                .apply(
                        BigQueryIO.writeTableRows()
                                .to(options.getOutput())
                                .withSchema(schema)
                                .withTestServices(testServices)
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
//        @Default.Enum("DIRECT_READ")
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
//        @Default.String(WEATHER_SAMPLES_SUMMARY_TABLE)
        @Default.String(WEATHER_SAMPLES_TABLE)
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