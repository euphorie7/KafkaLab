package lab.consumer.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class Consumer_1 {

    private static final Logger log =
            LoggerFactory.getLogger(Consumer_1.class);

    @KafkaListener(
            topics="EvennementMetier_1",
            groupId="ResponsabiliteMetier_1"
    )
    public void consume(ConsumerRecord<String,String> record) {
        log.info(
                "Message recu : topic={}, partition={}, offset={}, key={}, value={}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value()
        );
    }
}
