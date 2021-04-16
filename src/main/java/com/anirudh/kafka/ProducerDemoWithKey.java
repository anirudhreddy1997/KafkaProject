package com.anirudh.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class ProducerDemoWithKey {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger log = LoggerFactory.getLogger(ProducerDemoWithKey.class);
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

        for(int i=0 ; i<10;i++){

            //Records with same keys go to same partition everytime
            String topic = "Anirudh_first_topic";
            String key = "id_" + i;
            String value = "Kafka producers demo record" + i;
            log.info("Key " + key);
            //Create Producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>( topic , key, value );
            //send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if( e == null ){
                        log.info("Received Metadata \n"+
                                "Topic "+ recordMetadata.topic() + "\n" +
                                "Partition  " +  recordMetadata.partition() + "\n" +
                                "Offset " + recordMetadata.offset() + " \n" +
                                "Timestamp " + recordMetadata.timestamp() );
                    }else{
                        log.error("Exception occured "+e);
                    }
                }
            }).get();
            //.get() is to make syncronous send
        }

        //to flush the date, otherwise the send wont result in producing data as the app might close before sending (asynchronous)
        producer.flush();

        //to flush and close the producer
        producer.close();


    }
}
