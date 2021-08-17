package com.example.clickproducer;

import lombok.AllArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@AllArgsConstructor
public class ClickController {

    private final KafkaTemplate<String, PageClickEvent> kafkaTemplate;

    public static final String TOPIC_NAME = "pageClickTopic";

    @PostMapping("/click")
    public void click(
            @RequestBody PageClickEvent pageClickEvent
    ) {
        kafkaTemplate.send(TOPIC_NAME, pageClickEvent);
    }
}
