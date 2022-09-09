package beta.kafka.api;

import beta.kafka.payload.*;
import beta.kafka.producer.Producer;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RequestMapping("api")
@RestController
public class Api {

    private Producer producer;

    public Api(Producer producer) {
        this.producer = producer;
    }

    @PostMapping("/publish")
    @ResponseStatus(HttpStatus.CREATED)
    public int postOrderMessage(@RequestBody Message message) {
        return producer.publish(message);
    }
}
