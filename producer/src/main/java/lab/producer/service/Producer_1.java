package lab.producer.service;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.support.SendResult;
import lab.producer.AbstractProducer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



@Component
public class Producer_1 extends AbstractProducer<String> {

    private static final Logger log =
            LoggerFactory.getLogger(Producer_1.class);

    public Producer_1(KafkaTemplate<String, String> kafkaTemplate) {
        super(kafkaTemplate);
    }

    @Override
    protected String getTopic() {
        return "EvennementMetier_1";
    }

    @Override
    public String process(String key, String message) {


        log.info("Début de publication Kafka : topic={}, key={}",
                getTopic(), key);

        CompletableFuture<SendResult<String, String>>  future = publish(key, message);

        log.info("Attendre la response future (join)");
        // equivalent de await en typescrit c'est join
        SendResult<String, String> result = future.join();

        RecordMetadata metadata = result.getRecordMetadata();
        log.info(
                "Publication confirmée : topic={}, partition={}, offset={}",
                metadata.topic(),
                metadata.partition(),
                metadata.offset()
        );

        return "Message envoye avec succes : " + metadata;

    }
    public String process(String message) {

        int elementMetier_id = 123;
        log.info("Début de publication Kafka : topic={}, key={}",
                getTopic(), elementMetier_id);

        CompletableFuture<SendResult<String, String>>  future = publish(String.valueOf(elementMetier_id), message);

        log.info("Attendre la response future (join)");
        // equivalent de await en typescrit c'est join
        SendResult<String, String> result = future.join();

        RecordMetadata metadata = result.getRecordMetadata();
        log.info(
                "Publication confirmée : topic={}, partition={}, offset={}",
                metadata.topic(),
                metadata.partition(),
                metadata.offset()
        );

        return "Message envoye avec succes : " + metadata;

    }
}
