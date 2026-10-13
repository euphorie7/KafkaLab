package lab.producer;

import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

public abstract class AbstractProducer<T> {

    private final KafkaTemplate<String, T> kafkaTemplate;

    protected AbstractProducer(KafkaTemplate<String , T> kafkaTemplate){
        this.kafkaTemplate = kafkaTemplate;
    }

    protected abstract String getTopic();

    // avec cle
    public CompletableFuture<SendResult<String, T>> publish(String key , T event) {
        return kafkaTemplate.send(getTopic(), key, event);
    }

    // snas cle
    public CompletableFuture<SendResult<String, T>> publish(T event) {
        return kafkaTemplate.send(getTopic(), event);
    }

    public abstract String process(String message);


}
