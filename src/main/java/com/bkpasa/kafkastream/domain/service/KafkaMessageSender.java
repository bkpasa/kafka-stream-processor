package com.bkpasa.kafkastream.domain.service;

import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

public class KafkaMessageSender implements IKafkaSender {

    private final static Logger LOG = LoggerFactory.getLogger(KafkaMessageSender.class);

    @Autowired
    KafkaTemplate<Object, Object> kafkaTemplate;

    @Override
    public void send(String topic, Object key, Object value) {
        try {
            kafkaTemplate.send(topic, key, value).get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("could not send to kafka", e);
        }
    }

}
