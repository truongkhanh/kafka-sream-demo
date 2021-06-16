import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateItemProcessor implements Processor<String, ItemUpdate> {
  private ProcessorContext context;
  private static final Logger LOG = LoggerFactory.getLogger(UpdateItemProcessor.class);

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public void process(final String key, ItemUpdate value) {
    LOG.info("update database for new item {}", value);
    context.forward(value.userId, String.valueOf(System.currentTimeMillis()));
  }

  @Override
  public void close() {

  }
}
