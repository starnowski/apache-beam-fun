package com.github.starnowski.apache.beam.fun;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.tika.Tika;
import org.apache.tika.detect.EncodingDetector;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.txt.UniversalEncodingDetector;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.jboss.byteman.contrib.bmunit.WithByteman;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.channels.Channel;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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


    @Timeout(unit = TimeUnit.MINUTES, value = 10)
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
        Assertions.assertEquals(expectedIds, actualIds);
    }

    @Timeout(unit = TimeUnit.MINUTES, value = 10)
    @Test
    public void testReadFromUTF16EncodedFile() throws URISyntaxException, IOException {
        // Upload file
        // Create a test pipeline.
        Pipeline p = Pipeline.create();


        PCollection<String> begin = p
//                .apply(TextIO.read().from( this.getClass().getResource("json-data-utf-16.json").toURI().toString()));
                .apply("Find files", FileIO.match().filepattern(this.getClass().getResource("json-data-utf-16.json").toURI().toString()))
                .apply("Read file", FileIO.readMatches())
                .apply("Convert files to utf-8", ParDo.of(new ConvertFromUtf16ToUtf8()))
                .apply(new ByteArrayToLines())
                ;
        begin.apply("ParseJson", ParDo.of(new ParseJsonFn()))
//                .setCoder(JsonNodeCoder.of())
                .apply("PrintJson", ParDo.of(new CollectIdAndPassThrough()))
                .apply("PrintJson", ParDo.of(new PrintToConsole()));

        // WHEN
        p.run().waitUntilFinish();

        // THEN
        Assertions.assertEquals(expectedIds, actualIds);
    }

    @Timeout(unit = TimeUnit.MINUTES, value = 10)
    @ParameterizedTest
    @ValueSource(strings = {"json-data-utf-16.json", "json-data.json"})
    public void testReadFilesWithDifferentEncoding(String inputFileName) throws URISyntaxException, IOException {
        // Upload file
        // Create a test pipeline.
        Pipeline p = Pipeline.create();


        PCollection<String> begin = p
//                .apply(TextIO.read().from( this.getClass().getResource("json-data-utf-16.json").toURI().toString()));
                .apply("Find files", FileIO.match().filepattern(this.getClass().getResource(inputFileName).toURI().toString()))
                .apply("Read file", FileIO.readMatches())
                .apply("Convert files to utf-8", ParDo.of(new SmartFileConverter()))
                .apply(new ByteArrayToLines())
                ;
        begin.apply("ParseJson", ParDo.of(new ParseJsonFn()))
//                .setCoder(JsonNodeCoder.of())
                .apply("PrintJson", ParDo.of(new CollectIdAndPassThrough()))
                .apply("PrintJson", ParDo.of(new PrintToConsole()));

        // WHEN
        p.run().waitUntilFinish();

        // THEN
        Assertions.assertEquals(expectedIds, actualIds);
    }

    static class ByteArrayToLines extends PTransform<PCollection<byte[]>, PCollection<String>> {

        @Override
        public PCollection<String> expand(PCollection<byte[]> input) {
            return input.apply("Split ByteArray to Lines", ParDo.of(new DoFn<byte[], String>() {
                @ProcessElement
                public void processElement(@Element byte[] element, OutputReceiver<String> receiver) {
                    try (BufferedReader reader = new BufferedReader(
                            new InputStreamReader(new ByteArrayInputStream(element), StandardCharsets.UTF_8))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            receiver.output(line);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to parse byte array to lines", e);
                    }
                }
            }));
        }
    }

    static class ConvertFromUtf16ToUtf8 extends DoFn<FileIO.ReadableFile, byte[]> {

        @ProcessElement
        public void processElement(@Element FileIO.ReadableFile element, OutputReceiver<byte[]> out) {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(Channels.newInputStream(element.openSeekable()), StandardCharsets.UTF_16));
                 ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                 BufferedWriter writer = new BufferedWriter(
                         new OutputStreamWriter(byteArrayOutputStream, StandardCharsets.UTF_8))) {

                String line;
                while ((line = reader.readLine()) != null) {
                    writer.write(line);
                    writer.newLine();
                }
                writer.flush();
                out.output(byteArrayOutputStream.toByteArray());
                System.out.println("File conversion completed successfully.");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static class SmartFileConverter extends DoFn<FileIO.ReadableFile, byte[]> {

        @ProcessElement
        public void processElement(@Element FileIO.ReadableFile element, OutputReceiver<byte[]> out) {
            Charset detectedCharset = null;
            try (InputStream stream = Channels.newInputStream(element.openSeekable());
                 BufferedInputStream bufferedInputStream = new BufferedInputStream(stream)) {
                EncodingDetector encodingDetector = new UniversalEncodingDetector();
                // Create a ByteArrayInputStream from the byte array
                detectedCharset = encodingDetector.detect(bufferedInputStream, new Metadata());
                System.out.println("detectedCharset="+detectedCharset.displayName());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(Channels.newInputStream(element.openSeekable()), detectedCharset == null ? StandardCharsets.UTF_8 : detectedCharset));
                 ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                 BufferedWriter writer = new BufferedWriter(
                         new OutputStreamWriter(byteArrayOutputStream, StandardCharsets.UTF_8))) {

                String line;
                while ((line = reader.readLine()) != null) {
                    writer.write(line);
                    writer.newLine();
                }
                writer.flush();
                out.output(byteArrayOutputStream.toByteArray());
                System.out.println("File conversion completed successfully.");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
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