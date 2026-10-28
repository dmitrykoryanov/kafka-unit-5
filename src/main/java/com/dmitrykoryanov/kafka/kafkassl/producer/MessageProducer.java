package com.dmitrykoryanov.kafka.kafkassl.producer;

import com.dmitrykoryanov.kafka.kafkassl.config.KafkaTopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Продюсер (User:producer): после старта приложения отправляет по 10 сообщений в каждый топик.
 * Результат каждой отправки логирует {@link SendResultLogger}.
 */
@Component
@ConditionalOnProperty(prefix = "app.producer", name = "enabled", havingValue = "true", matchIfMissing = true)
public class MessageProducer implements ApplicationRunner {
    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);
    private static final int MESSAGES_PER_TOPIC = 10;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public MessageProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void run(ApplicationArguments args) {
        for (String topic : List.of(KafkaTopicConfig.TOPIC_1, KafkaTopicConfig.TOPIC_2)) {
            for (int i = 1; i <= MESSAGES_PER_TOPIC; i++) {
                String key = "key-" + i;
                String value = "Message " + i + " to " + topic;
                logger.info("Sending message: topic={}, key={}, value={}", topic, key, value);
                try {
                    kafkaTemplate.send(topic, key, value);
                } catch (Exception e) {
                    // Ошибки до отправки в брокер (создание продюсера, сериализация и т.п.)
                    logger.error("Error while sending message: topic={}, key={}, value={}", topic, key, value, e);
                }
            }
        }
        kafkaTemplate.flush();
    }
}
