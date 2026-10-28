package com.dmitrykoryanov.kafka.kafkassl.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Топики из задания. KafkaAdmin создаёт их при старте приложения от имени User:admin.
 * Права доступа (ACL) к ним выдаются вручную через kafka-acls, см. COMMANDS.md.
 */
@Configuration
public class KafkaTopicConfig {
    public static final String TOPIC_1 = "topic-1";
    public static final String TOPIC_2 = "topic-2";

    private static final int PARTITIONS = 3;
    private static final int REPLICAS = 3;
    private static final String MIN_INSYNC_REPLICAS = "2";

    @Bean
    public NewTopic topic1() {
        return buildTopic(TOPIC_1);
    }

    @Bean
    public NewTopic topic2() {
        return buildTopic(TOPIC_2);
    }

    private static NewTopic buildTopic(String name) {
        return TopicBuilder.name(name)
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_INSYNC_REPLICAS)
                .build();
    }
}
