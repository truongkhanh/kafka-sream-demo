import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTest.class);

  private static Topology topology;
  private static TestInputTopic<String, String> inputTopic;
  private static TestOutputTopic<String, String> outputTopic;

  @BeforeClass
  public static void setup() {
    topology = Main.buildTopology();
    final Properties props = Main.buildProperties();
    TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);
    inputTopic = testDriver.createInputTopic("new-item-updates", Serdes.String().serializer(),
        Serdes.String().serializer());
    outputTopic = testDriver
        .createOutputTopic("user-last-item-updated", Serdes.String().deserializer(),
            Serdes.String().deserializer());
    LOG.info("\n" + topology.describe().toString());
  }

  @Test
  public void firstUpdateIsSuccessful() {
    Long eventTimestamp = System.currentTimeMillis();
    inputTopic.pipeInput("item1",
        "{\"itemId\": \"item1\", \"userId\": \"user1\", \"itemDescription\": \"item 1 first description\"}",
        eventTimestamp);

    assertFalse(outputTopic.isEmpty());
    var record = outputTopic.readKeyValue();
    assertEquals(record.key, "user1");
    assertEquals(eventTimestamp, Long.valueOf(record.value));
  }

  @Test
  public void updateWithBlacklistKeywordIsRejected() {
    inputTopic.pipeInput("item1",
        "{\"itemId\": \"item1\", \"userId\": \"user1\", \"itemDescription\": \"item 1 first description BLACKLIST_KEYWORD\"}");

    assertTrue(outputTopic.isEmpty());
  }

  @Test
  public void tooCloseUpdateIsRejected() {
    var now = System.currentTimeMillis();
    inputTopic.pipeInput("item1",
        "{\"itemId\": \"item1\", \"userId\": \"user2\", \"itemDescription\": \"item 1 first description\"}",
        now);
    inputTopic.pipeInput("item2",
        "{\"itemId\": \"item2\", \"userId\": \"user2\", \"itemDescription\": \"item 2 first description\"}",
        now + 1);

    assertFalse(outputTopic.isEmpty());
    assertEquals(1, outputTopic.getQueueSize());
  }

  @Test
  public void notTooCloseUpdateIsAccepted() {
    var now = System.currentTimeMillis();
    inputTopic.pipeInput("item1",
        "{\"itemId\": \"item1\", \"userId\": \"user3\", \"itemDescription\": \"item 1 first description\"}",
        now);
    inputTopic.pipeInput("item2",
        "{\"itemId\": \"item2\", \"userId\": \"user3\", \"itemDescription\": \"item 2 first description\"}",
        now + 10000);

    assertFalse(outputTopic.isEmpty());
    var records = outputTopic.readRecordsToList();
    assertEquals(2, records.size());
  }
}
