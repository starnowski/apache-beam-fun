package com.github.starnowski.apache.beam.fun;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
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
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@WithByteman
public class ReadAndConvertFromGCSTest {

    // Our static input data, which will make up the initial PCollection.
    static final String[] WORDS_ARRAY = new String[]{
            "hi", "there", "hi", "hi", "sue", "bob",
            "hi", "sue", "", "", "ZOW", "bob", ""};
    static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);
    private static final Logger LOGGER = LoggerFactory.getLogger(ReadAndConvertFromGCSTest.class);
    private static final String PROJECT_ID = "test-project";
    private static final String BUCKET = "test-bucket";
    private static final GenericContainer<?> csContainer = new GenericContainer<>("fsouza/fake-gcs-server")
            .withEnv("GCS_BUCKETS", BUCKET)
            .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint(
                    "/bin/fake-gcs-server",
                    "-log-level", "debug",
                    "-scheme", "http"
            ))
            .withExposedPorts(4443);
    private static Storage storage;

    @BeforeAll
    public static void setupGCSContent() throws IOException, InterruptedException {
        csContainer.setLogConsumers(Arrays.asList(outputFrame -> System.out.println("[Cloud storage]" + outputFrame.getUtf8String())));
        csContainer.start();
        System.out.println("host: " + csContainer.getHost() + ":" + csContainer.getMappedPort(4443));

        storage = StorageOptions.newBuilder()
                .setHost(generateGCSHost())
                .setCredentials(NoCredentials.getInstance())
                .setProjectId(PROJECT_ID)
                .build().getService();

        storage.create(BucketInfo.of(BUCKET));
    }


    @AfterAll
    public static void tearDown() {
        csContainer.stop();
        System.out.println("Cloud storage logs: " + csContainer.getLogs());
    }

    @Timeout(unit = TimeUnit.MINUTES, value = 2, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    @Test

    //GoogleCloudStorageOptions
    //com.google.cloud.hadoop.gcsio
    //getStorageRootUrl()
    @BMUnitConfig(verbose = true, bmunitVerbose = true)
    @BMRules(rules = {
            @BMRule(name = "GoogleCloudStorageOptions getStorageRootUrl()",
                    targetClass = "com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions",
                    targetMethod = "getStorageRootUrl",
                    targetLocation = "AT ENTRY",
                    helper = "com.github.starnowski.apache.beam.fun.BMUnitHelperWithStaticStringProperty",
                    action = "RETURN getStaticStringProperty()")
            ,
            @BMRule(name = "Builder getStorageRootUrl()",
                    targetClass = "com.google.api.services.storage.Storage$Builder",
                    targetMethod = "chooseEndpoint(com.google.api.client.http.HttpTransport)",
                    targetLocation = "AT ENTRY",
                    helper = "com.github.starnowski.apache.beam.fun.BMUnitHelperWithStaticStringProperty",
                    action = "RETURN getStaticStringProperty()")

            //chooseEndpoint
    })
    public void testReadFromFile() throws URISyntaxException, IOException {
        // Upload file
        String csbObjectName = "some_dir/test.json";
        BlobId blobId = BlobId.of(BUCKET, csbObjectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        Blob blob = storage.create(blobInfo, Files.readAllBytes(Path.of(this.getClass().getResource("json-data.json").toURI())));
        System.out.println("GCS path: " + blob.getBlobId().toGsUtilUri());

        // Create a test pipeline.
        Pipeline p = Pipeline.create();
        BMUnitHelperWithStaticStringProperty.setStaticStringProperty(generateGCSHost());

        PCollection<String> begin = p.apply(TextIO.read().from(String.format("gs://%s/%s", BUCKET, csbObjectName)));

        begin.apply("ParseJson", ParDo.of(new ParseJsonFn()))
//                .setCoder(JsonNodeCoder.of())
                .apply("PrintJson", ParDo.of(new PrintToConsole()));
        PipelineResult.State state = p.run().waitUntilFinish();
    }

    private static String generateGCSHost() {
        return "http://" + csContainer.getHost() + ":" + csContainer.getMappedPort(4443);
    }

    static class ParseJsonFn extends DoFn<String, String> {
        private static final ObjectMapper objectMapper = new ObjectMapper();

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> out) {
            try {
                JsonNode jsonObject = objectMapper.readTree(element);
                out.output(jsonObject.toPrettyString());  // Output parsed JSON object
            } catch (IOException e) {
                // Handle the error if parsing fails
                System.err.println("Failed to parse JSON: " + element);
            }
        }
    }

    static class PrintToConsole extends DoFn<String, Void> {
        private static final ObjectMapper objectMapper = new ObjectMapper();

        @ProcessElement
        public void processElement(@Element String jsonObject) {
            System.out.println(jsonObject);  // Print each JSON object
        }
    }
}