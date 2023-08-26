package com.trungnguyen.kafka.demo.model.com.trungnguyen.kafka.demo;

import com.trungnguyen.kafka.demo.model.Avenger;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.*;

public class HelloWorldConsumer {
    private volatile boolean keepConsuming = true;

    public static void main(String[] args) {
        var kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "localhost:9092");
        kafkaProperties.put("group.id", "superhero_helloconsumer");
        kafkaProperties.put("enable.auto.commit", "true");
        kafkaProperties.put("auto.commit.interval.ms", "1000");
        kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProperties.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        kafkaProperties.put("schema.registry.url", "http://localhost:8081");
        kafkaProperties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        var consumer = new HelloWorldConsumer();
        consumer.consume(kafkaProperties);
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));
    }

    private void consume(Properties kafkaProperties) {
        try (KafkaConsumer<String, Avenger> consumer = new KafkaConsumer<>(kafkaProperties)) {
            consumer.subscribe(List.of("Superhero"));
            while (keepConsuming) {
                var records = consumer.poll(Duration.ofMillis(250));

                for (var record : records) {
                    record.offset();
                    System.out.println(record.value().toString());
                }
            }
        }
    }

    private void shutdown() {
        keepConsuming = false;
    }
}
