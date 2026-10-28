package com.dmitrykoryanov.kafka.kafkassl.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.event.ConsumerFailedToStartEvent;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

/**
 * Логирует остановку и сбой запуска контейнеров @KafkaListener,
 * например остановку консьюмера topic-2 из-за отказа в авторизации (reason=AUTH).
 */
@Component
public class ConsumerEventsLogger {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerEventsLogger.class);

    @EventListener
    public void onConsumerStopped(ConsumerStoppedEvent event) {
        String listenerId = event.getSource(MessageListenerContainer.class).getListenerId();
        if (event.getReason() == ConsumerStoppedEvent.Reason.NORMAL) {
            logger.info("Consumer stopped: listener={}", listenerId);
        } else {
            logger.error("Consumer stopped abnormally: listener={}, reason={}", listenerId, event.getReason());
        }
    }

    @EventListener
    public void onConsumerFailedToStart(ConsumerFailedToStartEvent event) {
        logger.error("Consumer failed to start: listener={}",
                event.getSource(MessageListenerContainer.class).getListenerId());
    }
}
