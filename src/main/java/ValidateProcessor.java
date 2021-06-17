import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValidateProcessor implements Processor<String, String> {

  public static final String BLACKLIST_KEYWORD = "BLACKLIST_KEYWORD";
  private static final Logger LOG = LoggerFactory.getLogger(ValidateProcessor.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private ReadOnlyKeyValueStore<String, String> globalAccountStateStore;
  private ProcessorContext context;

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
    globalAccountStateStore =
        (ReadOnlyKeyValueStore) context.getStateStore("userLastItemUpdated");
  }

  @Override
  public void process(final String key, String value) {
    try {
      var itemUpdate = MAPPER.readValue(value, ItemUpdate.class);
      if (this.isUpdateAllowed(itemUpdate)) {
        this.context.forward(key, itemUpdate);
      }
    } catch (JsonProcessingException e) {
      LOG.error("failed to handle message {}, {}, {}", key, value, e);
    }
  }

  private boolean isUpdateAllowed(ItemUpdate itemUpdate) {
    if (itemUpdate.itemDescription.contains(BLACKLIST_KEYWORD)) {

      LOG.info("reject update with blacklist keyword {}", itemUpdate);
      return false;
    }

    var lastUpdate = globalAccountStateStore.get(itemUpdate.userId);

    var result = !(lastUpdate != null && context.timestamp() - Long.valueOf(lastUpdate) < 1000);
    if (!result) {
      LOG.info("reject update as too close with last update {}", itemUpdate);
    }
    return result;
  }

  @Override
  public void close() {

  }
}
