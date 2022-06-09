package io.confluent.examples.streams;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import static io.confluent.examples.streams.IntegrationTestUtils.mkEntry;
import static io.confluent.examples.streams.IntegrationTestUtils.mkMap;
import static org.assertj.core.api.Assertions.assertThat;

public class countCountIntegrationTest {

  private static final String inputTopic = "inputTopic";
  private static final String outputTopic = "outputTopic";

  @Test
  public void shouldCountWords() {
    final List<String> inputValues = Arrays.asList(
      "Hello Kafka Streams",
      "All streams lead to Kafka",
      "Join Kafka Summit",
      "И теперь пошли русские слова"
    );
    final Map<String, Long> expectedWordCounts = mkMap(
      mkEntry("hello", 1L),
      mkEntry("all", 1L),
      mkEntry("streams", 2L),
      mkEntry("lead", 1L),
      mkEntry("to", 1L),
      mkEntry("join", 1L),
      mkEntry("kafka", 3L),
      mkEntry("summit", 1L),
      mkEntry("и", 1L),
      mkEntry("теперь", 1L),
      mkEntry("пошли", 1L),
      mkEntry("русские", 1L),
      mkEntry("слова", 1L)
    );

    final Serde<String> stringSerde = Serdes.String();
    final Serde<Long> longSerde = Serdes.Long();

    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-lambda-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, String> textLines = builder.stream(inputTopic);

    final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

    final KTable<String, Long> wordCounts = textLines
      .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
      .groupBy((key, word) -> word)
      .count();

    wordCounts.toStream().to(outputTopic, Produced.with(stringSerde, longSerde));

    try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)) {
      final TestInputTopic<Void, String> input = topologyTestDriver
        .createInputTopic(inputTopic,
                          new IntegrationTestUtils.NothingSerde<>(),
                          new StringSerializer());
      final TestOutputTopic<String, Long> output = topologyTestDriver
        .createOutputTopic(outputTopic, new StringDeserializer(), new LongDeserializer());

      input.pipeValueList(inputValues);

      assertThat(output.readKeyValuesToMap()).isEqualTo(expectedWordCounts);
    }
  }
}
