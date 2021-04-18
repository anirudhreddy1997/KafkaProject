package com.anirudh.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "ConsumerDemoThreads";
        String topic = "Anirudh_first_topic";

        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating consumer thread");
        Runnable myConsumerThread = new ConsumerThread(latch, bootstrapServer, groupId, topic);
        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(
                () ->{
                    logger.info("Caught shutdown hook");
                    ((ConsumerThread)myConsumerThread).shutdown();
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    logger.info("Shutdown called");
                }
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            logger.info("Application is closing");
        }



    }

    public static class ConsumerThread implements Runnable{
        Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
        CountDownLatch latch;
        String bootstrapServer;
        String groupId ;
        String topic ;
        KafkaConsumer<String, String> consumer;

        public ConsumerThread(CountDownLatch latch, String bootstrapServer, String groupId, String topic) {
            this.latch = latch;
            this.bootstrapServer = bootstrapServer;
            this.groupId = groupId;
            this.topic = topic;

            //Consumer config
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // Create consumer
            consumer = new KafkaConsumer<>(properties);

            //subscribe consumer to topic
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {


            try{
                //poll to get the data
                while(true){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for(ConsumerRecord<String, String> record: records){
                        logger.info("Key: "+ record.key() + ", value:" +record.value());
                        logger.info("Partition: "+ record.partition() + ", Offset:" + record.offset());
                    }
                }
            }catch(WakeupException e){
                logger.info("Received shutdown signal");
            }finally{
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown(){
            consumer.wakeup();
        }
    }
}
