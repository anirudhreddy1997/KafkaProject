package com.anirudh.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092"; // port where we start kafka bootstrap server
        //kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic Anirudh_first_topic --group my-first-application

        //create producer properties -refer https://kafka.apache.org/documentation/#producerconfigs
        Properties properties =  new Properties();
        /*
        //Old way of adding properties

        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        */

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //Create Producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("Anirudh_first_topic", "Kafka producers first record");
        //send data
        producer.send(record);

        //to flush the date, otherwise the send wont result in producing data as the app might close before sending (asynchronous)
        producer.flush();

        //to flush and close the producer
        producer.close();


    }
}
