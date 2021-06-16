import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);
  public static void main(final String[] args) {
    final Topology builder = buildTopology();


    Properties props = buildProperties();
    final KafkaStreams streams = new KafkaStreams(builder, props);
    LOG.info(builder.describe().toString());
    streams.start();
  }

  public static Properties buildProperties() {
    final Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-demo");
    props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    return props;
  }

  public static Topology buildTopology() {
    final Topology builder = new Topology();

    final String sourceName = "New Item Source Node";
    final String validateProcessorName = "Validate Item Node";
    final String updateItemProcessorName = "Update Item Node";
    final String sinkName = "Last Update Per User";

    final String userLastItemUpdatedTopic = "userLastItemUpdated";
    final String userLastItemUpdatedGlobalStore = "userLastItemUpdated";
    final String userLastItemUpdatedSourceNode = "userLastItemUpdated";

    final var userLastItemUpdatedGlobalStoreBuilder = Stores.keyValueStoreBuilder(
        Stores.inMemoryKeyValueStore(userLastItemUpdatedGlobalStore),
        Serdes.String(),
        Serdes.String()).withLoggingDisabled();;


    builder.addSource(sourceName, "new-items");

    builder.addProcessor(validateProcessorName, () -> new ValidateProcessor(), sourceName);
    builder.addProcessor(updateItemProcessorName, () -> new UpdateItemProcessor(), validateProcessorName);
    builder.addSink(
        sinkName,
        userLastItemUpdatedTopic,
        Serdes.String().serializer(),
        Serdes.String().serializer(),
        updateItemProcessorName);

    builder.addGlobalStore(
        userLastItemUpdatedGlobalStoreBuilder,
        userLastItemUpdatedSourceNode,
        new StringDeserializer(),
        new StringDeserializer(),
        userLastItemUpdatedTopic,
        "GlobalLastItemUpdateByUserProcessor",
        () ->
            new GlobalStateProcessor<String, String>(userLastItemUpdatedGlobalStore));
    return builder;
  }
}
