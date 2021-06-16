import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Test;

public class IntegrationTest {
  @Test
  public void testTopology() {
    Topology topology = Main.buildTopology();
    final Properties props =Main.buildProperties();
    TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);
    TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("new-items", Serdes.String().serializer(), Serdes.String().serializer());

    TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic("userLastItemUpdated", Serdes.String().deserializer(), Serdes.String().deserializer());
    var beforeInput = System.currentTimeMillis();
    inputTopic.pipeInput("item1", "{\"userId\": \"user1\", \"itemDescription\": \"first description\"}");

    var record = outputTopic.readKeyValue();
    assertEquals(record.key, "user1");
    assertTrue(Long.valueOf(record.value) > beforeInput);
  }
}
