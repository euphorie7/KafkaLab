package lab.producer.configuration;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic evenementMetierTopic() {
        return new NewTopic(
                "EvenementMetier_1",
                1,
                (short) 1
        );
    }
}
