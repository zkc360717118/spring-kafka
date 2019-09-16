package com.controller.kafkaconsume.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer2 {
    private static final Logger log= LoggerFactory.getLogger(KafkaConsumer2.class);

    //声明consumerID为demo，监听topicName为topic.quick.demo的Topic
    @KafkaListener(id = "demo", topics = "test")
    public void listen(String msgData) {
        log.info("demo receive : "+msgData);
    }
}
