package com.bkpasa.kafkastream.domain.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.bkpasa.kafkastream.BootstrapServers;
import com.bkpasa.kafkastream.MessageException;
import com.bkpasa.kafkastream.TopicNames;
import com.bkpasa.kafkastream.domain.model.Alert;
import com.bkpasa.kafkastream.domain.model.AlertKey;

public class AlertsStreamService {
    private final static Logger LOGGER = LoggerFactory.getLogger(AlertsStreamService.class);
    private final static int STORE_WAIT_TIME = 2000;
    private KafkaStreams kafkaStreams = null;

    @Autowired
    private KafkaMessageSender kafkaMessageSender;

    @Autowired
    private Serde<Alert> alertSerde;

    @Autowired
    private Serde<AlertKey> alertKeySerde;

    @PostConstruct
    public void init() {
        LOGGER.info("Starting AlertService ...");
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "alert-stream-service_18_03_2019");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers.servers);

        StoreBuilder<KeyValueStore<AlertKey, Alert>> keyValueStoreBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("alertsProcessorState"), alertKeySerde,
                        alertSerde);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(keyValueStoreBuilder);

        KStream<AlertKey, Alert> alertStream =
                builder.stream(TopicNames.SNAPSHOT_TOPIC, Consumed.with(alertKeySerde, alertSerde));

        KGroupedStream<AlertKey, Alert> groupedAlertStream =
                alertStream.groupByKey(Serialized.with(alertKeySerde, alertSerde));
        // groupedAlertStream.count().toStream( ).to("streams-alert-count", Produced.with(alertKeySerde,
        // Serdes.Long()));

        groupedAlertStream.count().toStream().foreach((k, c) -> {
            System.out.println("**** key: " + k + " count:" + c);
        });

        alertStream.process(new ProcessorSupplier<AlertKey, Alert>() {
            @Override
            public Processor<AlertKey, Alert> get() {
                return new AlertProcessor<>();
            }
        }, "alertsProcessorState");

        // alertStream.foreach((key, alert) -> {
        // System.out.println("key=" + key + " value=" + alert);
        //
        // });

        final Topology topology = builder.build();
        kafkaStreams = new KafkaStreams(topology, props);

        kafkaStreams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            LOGGER.error("stream exception occurred. thread id=" + thread.getId(), throwable);
        });

        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        LOGGER.info("AlertsSnapshotStatusService started ...");
    }

    @PreDestroy
    public void cleanup() {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
    }

    private <T> T waitUntilStoreIsQueryable(final String storeName, final QueryableStoreType<T> queryableStoreType)
            throws MessageException {
        int waitTimeout = STORE_WAIT_TIME;
        while (true) {
            try {
                return kafkaStreams.store(storeName, queryableStoreType);
            } catch (InvalidStateStoreException ignored) {
                // store not yet ready for querying
                try {
                    Thread.sleep(100);
                    waitTimeout -= 100;
                    if (waitTimeout <= 0) {
                        throw new MessageException("Timeout while trying to get the materialized store :" + storeName,
                                null);
                    }
                } catch (InterruptedException e) {
                    LOGGER.error("Interrupted while waiting state store", e);
                    throw new MessageException("Unable to get the materialized store :" + storeName, e);
                }
            }
        }
    }

    private <K, V> V getAlertByKey(String storeName, K key) {
        try {
            final ReadOnlyKeyValueStore<K, V> store =
                    waitUntilStoreIsQueryable(storeName, QueryableStoreTypes.keyValueStore());
            return store.get(key);
        } catch (MessageException e) {
            LOGGER.error("could not query the materialized store for key = " + key, e);
            return null;
        }
    }

    public <K, V> List<V> getAll(String storeName) {
        try {
            final ReadOnlyKeyValueStore<K, V> store =
                    waitUntilStoreIsQueryable(storeName, QueryableStoreTypes.keyValueStore());
            final KeyValueIterator<K, V> range = store.all();
            final List<KeyValue<K, V>> results = new ArrayList<>();
            while (range.hasNext()) {
                final KeyValue<K, V> next = range.next();
                results.add(next);
            }
            return results.stream().map(m -> m.value).collect(Collectors.toList());
        } catch (Exception e) {
            LOGGER.error("could not query the materialized store.", e);
            return new ArrayList<>();
        }
    }

    public List<Alert> getAlerts() {
        return getAll("alertsProcessorState");
    }

    public List<Alert> getAlert(String storeName, AlertKey key) {
        return getAlertByKey("alertsProcessorState", key);
    }
}
