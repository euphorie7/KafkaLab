package lab.producer.service;

import org.springframework.kafka.support.SendResult;
import lab.producer.AbstractProducer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.concurrent.CompletableFuture;



@Component
public class Producer_1 extends AbstractProducer<String> {


    public Producer_1(KafkaTemplate<String, String> kafkaTemplate) {
        super(kafkaTemplate);
    }

    @Override
    protected String getTopic() {
        return "EvennementMetier_1";
    }

    @Override
    public String process(String message) {

        int elementMetier_id = 123;
        CompletableFuture<SendResult<String, String>>  future = publish(String.valueOf(elementMetier_id), message);

        // equivalent de await en typescrit c'est join
        SendResult<String, String> result = future.join();

        ProducerRecord<String, String> metadata = result.getProducerRecord();

        return "Message envoye avec succes : " + metadata;

    }
}
