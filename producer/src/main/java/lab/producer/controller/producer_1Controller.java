package lab.producer.controller;


import lab.producer.service.Producer_1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.CompletionException;
record Request (
    String key,
    String message
){}

@RestController
@RequestMapping("api/producer_1")
public class producer_1Controller {
    private static final Logger log =
            LoggerFactory.getLogger(producer_1Controller.class);
    private final Producer_1 producer_1;

    private producer_1Controller(Producer_1 producer_1){
        this.producer_1 = producer_1;
    }

    @PostMapping("lance_le_process")
    public ResponseEntity<String> lancer_le_process(
                                                    @RequestBody Request body) {

        try {
            return ResponseEntity.ok(producer_1.process(body.key(),body.message()));
        }
        catch(CompletionException e) {
            log.error("Échec de publication Kafka ", e);
        return ResponseEntity
                .status(HttpStatus.SERVICE_UNAVAILABLE)
                .body("Erreur de publication Kafka : "
                        + e.getCause().getMessage());
    }
        catch(Exception e ){
        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("Erreur" + e.getMessage());
    }
    }


}
