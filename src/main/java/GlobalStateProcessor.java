import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GlobalStateProcessor<K, V> implements Processor<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(GlobalStateProcessor.class);
  private final String storeName;

  private KeyValueStore<K, V> globalStore;

  public GlobalStateProcessor(String storeName) {
    this.storeName = storeName;
  }

  @Override
  public void init(ProcessorContext context) {
    this.globalStore = (KeyValueStore) context.getStateStore(storeName);
  }

  @Override
  public void process(final K key, V value) {
    globalStore.put(key, value);
    LOG.debug("Updated {} - key: {} value: {}", storeName, key, value);
  }

  @Override
  public void close() {
  }
}
