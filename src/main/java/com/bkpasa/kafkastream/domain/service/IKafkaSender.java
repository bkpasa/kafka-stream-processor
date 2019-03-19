package com.bkpasa.kafkastream.domain.service;

public interface IKafkaSender {
    public void send(String topic, Object key, Object value);
}
