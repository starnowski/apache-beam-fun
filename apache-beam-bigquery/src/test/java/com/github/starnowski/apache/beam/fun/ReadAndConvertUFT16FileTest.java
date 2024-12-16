package com.github.starnowski.apache.beam.fun;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.jboss.byteman.contrib.bmunit.WithByteman;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ReadAndConvertUFT16FileTest {

    // Our static input data, which will make up the initial PCollection.
    private static final Set<String> expectedIds = new HashSet<>(Arrays.asList("1234", "cxczvas", "3213"));
    private static final HashSet<String> actualIds = new HashSet<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(ReadAndConvertUFT16FileTest.class);

    @BeforeEach
    public void beforeTest() {
        actualIds.clear();
    }


    @Timeout(unit = TimeUnit.MINUTES, value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    @Test
    public void testReadFromFile() throws URISyntaxException, IOException {
        // Upload file
        // Create a test pipeline.
        Pipeline p = Pipeline.create();

        PCollection<String> begin = p.apply(TextIO.read().from( this.getClass().getResource("json-data.json").toURI().toString()));

        begin.apply("ParseJson", ParDo.of(new ParseJsonFn()))
//                .setCoder(JsonNodeCoder.of())
                .apply("PrintJson", ParDo.of(new CollectIdAndPassThrough()))
                .apply("PrintJson", ParDo.of(new PrintToConsole()));

        // WHEN
        p.run().waitUntilFinish();

        // THEN
        Assertions.assertArrayEquals(actualIds.toArray(), expectedIds.toArray());
    }

    static class ParseJsonFn extends DoFn<String, String> {
        private static final ObjectMapper objectMapper = new ObjectMapper();

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> out) {
            try {
                JsonNode jsonObject = objectMapper.readTree(element);
                out.output(jsonObject.get("id").textValue());  // Output parsed JSON object
            } catch (IOException e) {
                // Handle the error if parsing fails
                System.err.println("Failed to parse JSON: " + element);
            }
        }
    }

    static class PrintToConsole extends DoFn<String, Void> {

        @ProcessElement
        public void processElement(@Element String jsonObject) {
            System.out.println(jsonObject);  // Print each JSON object
        }
    }

    static class CollectIdAndPassThrough extends DoFn<String, String> {

        @ProcessElement
        public void processElement(@Element String id, OutputReceiver<String> out) {
            actualIds.add(id);
            out.output(id);
        }
    }
}