package com.trungnguyen.kafka.demo.model.com.trungnguyen.kafka.demo;

import com.trungnguyen.kafka.demo.model.Avenger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

public class HelloWordProducer {
    public static void main(String[] args) {
        var kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "localhost:9092");
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        kafkaProperties.put("schema.registry.url", "http://localhost:8081");

        try (Producer<String, Avenger> producer = new KafkaProducer<>(kafkaProperties)) {
            var avenger = new Avenger("Iron Man", "Tony Stark", List.of("Iron Man"));
            var record = new ProducerRecord<String, Avenger>("Superhero", avenger);
            producer.send(record);
        }
    }
}
