package com.bkpasa.kafkastream.domain.service;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueStore;

public class AlertProcessor<K, V> implements Processor<K, V> {

    // private StateStore store;
    private KeyValueStore<K, V> store;

    @Override
    public void init(ProcessorContext context) {
        this.store = (KeyValueStore<K, V>) context.getStateStore("alertsProcessorState");
        context.schedule(5000, PunctuationType.WALL_CLOCK_TIME, new Punctuator() {
            @Override
            public void punctuate(long timestamp) {
                System.out.println(store.name() + " has approximately " + store.approximateNumEntries() + " records");
            }
        });
    }

    @Override
    public void process(K key, V value) {
        System.out.println("Received key:" + key + " and value: " + value);
        this.store.put(key, value);
    }

    @Override
    public void punctuate(long timestamp) {

    }

    @Override
    public void close() {

    }

}
