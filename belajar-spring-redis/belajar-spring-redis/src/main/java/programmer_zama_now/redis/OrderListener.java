package programmer_zama_now.redis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class OrderListener implements StreamListener<String, ObjectRecord<String, Order>> {

    @Override
    public void onMessage(ObjectRecord<String, Order> message) {

        Order order = message.getValue();
        log.info("Received order: {}", order);
    }
}
