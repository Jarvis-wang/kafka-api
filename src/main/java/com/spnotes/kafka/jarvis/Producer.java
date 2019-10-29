package com.spnotes.kafka.jarvis;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Scanner;

/**
 * Created by jarvis on 12/28/19.
 */
public class Producer {
    private static Scanner in;
    public static void main(String[] argv)throws Exception {
        if (argv.length != 1) {
            System.err.println("Please specify 1 parameter with the topic name. ");
            System.exit(-1);
        }
        String topicName = argv[0];
        in = new Scanner(System.in);
        System.out.println("Enter message(type exit to quit)");

        //Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);

        ProducerRecord<String, String> rec1 = new ProducerRecord<String, String>(topicName,"J");
        producer.send(rec1);

        ProducerRecord<String, String> rec2 = new ProducerRecord<String, String>(topicName,"A");
        producer.send(rec2);

        ProducerRecord<String, String> rec3 = new ProducerRecord<String, String>(topicName,"R");
        producer.send(rec3);

        ProducerRecord<String, String> rec4 = new ProducerRecord<String, String>(topicName,"V");
        producer.send(rec4);

        ProducerRecord<String, String> rec5 = new ProducerRecord<String, String>(topicName,"I");
        producer.send(rec5);

        String line = in.nextLine();
        while(!line.equals("exit")) {
            //TODO: Make sure to use the ProducerRecord constructor that does not take parition Id
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName,line);
            producer.send(rec);
            line = in.nextLine();
        }
        in.close();
        producer.close();
    }
}
