package programmer_zama_now.redis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class CustomerListener implements MessageListener {

    @Override
    public void onMessage(Message message, byte[] pattern) {
        log.info("Receive message: {}", new String(message.getBody()));
    }
}
