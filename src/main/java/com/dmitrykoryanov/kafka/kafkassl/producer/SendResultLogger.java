package com.dmitrykoryanov.kafka.kafkassl.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.stereotype.Component;

/**
 * Логирует результат каждой отправки KafkaTemplate.
 * Заменяет стандартный LoggingProducerListener, который пишет только ошибки.
 */
@Component
public class SendResultLogger implements ProducerListener<Object, Object> {
    private static final Logger logger = LoggerFactory.getLogger(SendResultLogger.class);

    @Override
    public void onSuccess(ProducerRecord<Object, Object> record, RecordMetadata metadata) {
        logger.info("Message sent: topic={}, partition={}, offset={}, key={}, value={}",
                metadata.topic(), metadata.partition(), metadata.offset(), record.key(), record.value());
    }

    @Override
    public void onError(ProducerRecord<Object, Object> record, RecordMetadata metadata, Exception exception) {
        logger.error("Error while producing: topic={}, key={}, value={}",
                record.topic(), record.key(), record.value(), exception);
    }
}
