package com.dmitrykoryanov.kafka.kafkassl.consumer;

import com.dmitrykoryanov.kafka.kafkassl.config.KafkaTopicConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Консьюмер (User:consumer). Читает topic-1; на topic-2 у него есть только право на группу,
 * а чтение топика запрещено ACL, поэтому второй слушатель падает с TopicAuthorizationException и останавливается.
 * Группы должны совпадать с выданными через kafka-acls.
 */
@Component
public class MessageConsumer {
    private static final Logger logger = LoggerFactory.getLogger(MessageConsumer.class);

    public static final String TOPIC_1_GROUP = "topic-1-group";
    public static final String TOPIC_2_GROUP = "topic-2-group";

    @KafkaListener(id = "topic-1-listener", topics = KafkaTopicConfig.TOPIC_1, groupId = TOPIC_1_GROUP)
    public void consumeTopic1(ConsumerRecord<String, String> record) {
        logReceived(record);
    }

    @KafkaListener(id = "topic-2-listener", topics = KafkaTopicConfig.TOPIC_2, groupId = TOPIC_2_GROUP)
    public void consumeTopic2(ConsumerRecord<String, String> record) {
        logReceived(record);
    }

    private static void logReceived(ConsumerRecord<String, String> record) {
        logger.info("Received message: topic={}, partition={}, offset={}, key={}, value={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
    }
}
