package lab.consumer.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class Consumer {

    private static final Logger log =
            LoggerFactory.getLogger(Consumer.class);
    @Value("${spring.application.name}")
    private String consumerName;
    @Value("${app.kafka.topic}")
    private String consumerTopic;
    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupeId;

    @KafkaListener(topics="${app.kafka.topic}")
    public void consume(ConsumerRecord<String,String> record) {
        log.info(
                "Message recu :name={}, appTopic={},springGroupeId={}, springtopic={}, partition={}, offset={}, key={}, value={}",
                this.consumerName,
                record.topic(),
                this.consumerTopic,
                this.consumerGroupeId,
                record.partition(),
                record.offset(),
                record.key(),
                record.value()
        );
    }
}
