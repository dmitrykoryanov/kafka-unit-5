package com.dmitrykoryanov.kafka.kafkassl;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

// Проверяем только поднятие контекста: без создания топиков, консьюмеров и отправки, чтобы тест не требовал кластера
@SpringBootTest(properties = {
        "spring.kafka.admin.auto-create=false",
        "spring.kafka.listener.auto-startup=false",
        "app.producer.enabled=false"
})
class KafkaSslApplicationTests {

    @Test
    void contextLoads() {
    }

}
