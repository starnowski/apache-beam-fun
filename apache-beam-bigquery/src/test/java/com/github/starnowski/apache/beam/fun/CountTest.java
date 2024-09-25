package com.github.starnowski.apache.beam.fun;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.healthcare.v1.model.Field;
import com.google.api.services.pubsub.model.Schema;
import com.google.cloud.bigtable.data.v2.models.TableId;
import net.bytebuddy.utility.dispatcher.JavaDispatcher;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesImpl;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.BigQueryEmulatorContainer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CountTest {

    // Our static input data, which will make up the initial PCollection.
    static final String[] WORDS_ARRAY = new String[] {
            "hi", "there", "hi", "hi", "sue", "bob",
            "hi", "sue", "", "", "ZOW", "bob", ""};

    static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);
    private static BigQueryEmulatorContainer bigQueryContainer = new BigQueryEmulatorContainer("ghcr.io/goccy/bigquery-emulator:0.4.3");


    @Before
    public void setupDataLake() {
        bigQueryContainer.start();
//        clearDataSet();
//        createDataSet();
//        createTable();
    }

    @After
    public void tearDown() {
        bigQueryContainer.stop();
    }

    @Test
    public void testCount() {
        // Create a test pipeline.
        BigQueryOptions options = PipelineOptionsFactory.as(BigQueryOptions.class);
        options.setBigQueryEndpoint(bigQueryContainer.getEmulatorHttpEndpoint());
        options.setProject(bigQueryContainer.getProjectId());
        options.setTempLocation("nosuch_temp");
        Pipeline p = Pipeline.create(options);


        Options subOptions = PipelineOptionsFactory.create().as(Options.class);
        applyBigQueryTornadoes(p, subOptions);
        p.run().waitUntilFinish();
    }

    private static final String WEATHER_SAMPLES_TABLE =
            "apache-beam-testing.samples.weather_stations";

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