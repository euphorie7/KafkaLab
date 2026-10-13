package lab.producer.controller;


import lab.producer.service.Producer_1;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.CompletionException;


@RestController
@RequestMapping("api/producer_1")
public class producer_1Controller {

    private final Producer_1 producer_1;

    private producer_1Controller(Producer_1 producer_1){
        this.producer_1 = producer_1;
    }

    @PostMapping("lance_le_process")
    public ResponseEntity<String> lancer_le_process(@RequestBody String message) {

        try {
            return ResponseEntity.ok(producer_1.process(message));
        }
        catch(CompletionException e) {
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
