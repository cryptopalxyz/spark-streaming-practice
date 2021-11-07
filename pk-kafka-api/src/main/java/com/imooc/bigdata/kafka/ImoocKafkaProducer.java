package com.imooc.bigdata.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ImoocKafkaProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9093,localhost:9094");
        props.put("acks","all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i=0;i<100;i++) {
            producer.send(new ProducerRecord<>("mytopic",i+"",String.valueOf(i)));
        }
        System.out.println("发送完毕");
        producer.close();

    }

}
