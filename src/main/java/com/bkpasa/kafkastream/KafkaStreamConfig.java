package com.bkpasa.kafkastream;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.bkpasa.kafkastream.domain.model.Alert;
import com.bkpasa.kafkastream.domain.model.AlertKey;
import com.bkpasa.kafkastream.domain.service.AlertsStreamService;
import com.bkpasa.kafkastream.domain.service.KafkaMessageSender;

@Configuration
@EnableKafka
public class KafkaStreamConfig {

    @Bean
    public AlertsStreamService alertsService()
    {
        return new AlertsStreamService();
    }

    @Bean
    public <K, V> KafkaTemplate<K, V> kafkaTemplate() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers.servers);
        return new KafkaTemplate<>(
                new DefaultKafkaProducerFactory<>(props, keySerializer(), new JsonSerializer<V>()));
    }

    private <T> JsonSerializer<T> keySerializer() {
        JsonSerializer<T> ser = new JsonSerializer<>();
        ser.setUseTypeMapperForKey(true);
        return ser;
    }
    @Bean
    public KafkaMessageSender kafkaSender() {
        return new KafkaMessageSender();
    }
    // end kafka

    // kafka receiver


    // @Bean
    // public ConsumerFactory<AlertKey, Alert> consumerFactory() {
    // Map<String, Object> props = new HashMap<>();
    // props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // DefaultKafkaConsumerFactory<AlertKey, Alert> consumerFactory =
    // new DefaultKafkaConsumerFactory<>(props);
    // consumerFactory.setKeyDeserializer(alertKeySerde().deserializer());
    // consumerFactory.setValueDeserializer(alertSerde().deserializer());
    // return consumerFactory;
    // }
    //
    // @Bean
    // public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<AlertKey, Alert>>
    // kafkaListenerContainerFactory() {
    // ConcurrentKafkaListenerContainerFactory<AlertKey, Alert> factory =
    // new ConcurrentKafkaListenerContainerFactory<>();
    // factory.setConsumerFactory(consumerFactory());
    // return factory;
    // }

    // @Bean
    // public KafkaMessageReceiver kafkaMessageReceiver() {
    // return new KafkaMessageReceiver();
    // }

    @Bean
    public Serde<AlertKey> alertKeySerde(){
        return new JsonSerde<>(AlertKey.class).setUseTypeMapperForKey(true);
    }

    @Bean
    public Serde<Alert> alertSerde() {
        return new JsonSerde<>(Alert.class);
    }
    // end kafka
}
