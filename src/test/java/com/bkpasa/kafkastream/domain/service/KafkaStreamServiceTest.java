package com.bkpasa.kafkastream.domain.service;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.bkpasa.kafkastream.KafkaStreamConfig;
import com.bkpasa.kafkastream.TopicNames;
import com.bkpasa.kafkastream.domain.model.Alert;
import com.bkpasa.kafkastream.domain.model.AlertKey;
import com.bkpasa.kafkastream.domain.model.AlertType;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { KafkaStreamConfig.class })
public class KafkaStreamServiceTest {
    @Autowired
    private KafkaMessageSender kafkaMessageSender;

    @Autowired
    private AlertsStreamService alertsService;

    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void alertStreamServiceGetAlertsTest() throws InterruptedException {

        Alert alertBean =
                new Alert.Builder().alertType(AlertType.LIABILITY_ALERT).description("desc").enabled(true).eventId(1L)
                        .marketId(3L).build();
        AlertKey alertKey = new AlertKey.Builder().eventId(7L).marketId(6L).build();
        kafkaMessageSender.send(TopicNames.SNAPSHOT_TOPIC, alertKey, alertBean);

        alertBean =
                new Alert.Builder().alertType(AlertType.LIABILITY_ALERT).description("desc").enabled(true).eventId(1L)
                        .marketId(3L).build();
        alertKey = new AlertKey.Builder().eventId(6L).marketId(6L).build();
        kafkaMessageSender.send(TopicNames.SNAPSHOT_TOPIC, alertKey, alertBean);

        alertBean =
                new Alert.Builder().alertType(AlertType.LIABILITY_ALERT).description("desc33").enabled(true).eventId(1L)
                        .marketId(7L).build();
        alertKey = new AlertKey.Builder().eventId(16L).marketId(6L).build();
        kafkaMessageSender.send(TopicNames.SNAPSHOT_TOPIC, alertKey, alertBean);

        Thread.sleep(1500);
        List<Alert> alerts = alertsService.getAlerts();

        Assertions.assertThat(alerts.size()).isGreaterThan(0);
    }
}
