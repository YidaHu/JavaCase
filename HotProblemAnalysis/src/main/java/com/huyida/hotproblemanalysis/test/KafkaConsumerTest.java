package com.huyida.hotproblemanalysis.test;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

/**
 * @program: HotProblemAnalysis
 * @description:
 * @author: huyida
 * @create: 2022-06-20 17:14
 **/

public class KafkaConsumerTest {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerTest.class);
    private static final String KAFKA_SERVER = "127.0.0.1:9092";
    private static final String Topic = "topic-chat-record";
    private static final String ConsumerGroup = "consumer-test-qa";

    private static Properties buildProperties() {
        Properties prop = new Properties();

        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        prop.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);

        prop.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 32);
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); //若没有指定offset,默认从最后的offset(latest)开始；earliest表示从最早的offset开始
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, ConsumerGroup);
        prop.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 180 * 1000L);

        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return prop;
    }

    public static void main(String[] args) {
        Properties prop = buildProperties();
        @SuppressWarnings("resource")
        Consumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

        consumer.subscribe(Arrays.asList(Topic));
        ConsumerRecords<String, String> records = null;
        while (true) {
            records = consumer.poll(1000);
            if (records.count() <= 0) {
                continue;
            }
            logger.info("records count:" + records.count());
            for (ConsumerRecord<String, String> record : records) {
                logger.info("partition:{}, offset:{}, tostring:{}",
                        record.partition(), record.offset(), record.toString());
            }
        }
    }
}
