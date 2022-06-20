package com.huyida.hotproblemanalysis.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.huyida.hotproblemanalysis.domain.ChatRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * @program: HotProblemAnalysis
 * @description:
 * @author: huyida
 * @create: 2022-06-20 16:48
 **/

public class KafkaProducerTest {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerTest.class);
    private static final String KAFKA_SERVER = "127.0.0.1:9092";
    private static final String TOPIC = "topic-chat-record";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static Properties buildProperties(){
        Properties pro = new Properties();
        pro.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        pro.put(ProducerConfig.ACKS_CONFIG, "1"); //1:确保leader收到消息；0:消息发到发送缓冲区即可；-1/all:确保leader收到消息并且同步给follower
        pro.put(ProducerConfig.RETRIES_CONFIG, "1");
        pro.put(ProducerConfig.LINGER_MS_CONFIG, 3); //提高生产效率，尤其是在同步生产时
        pro.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 64);
        pro.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 64 * 1024 * 1024);
        pro.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, 64 * 1024 * 1024);
        pro.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");  //推荐使用snappy或lz4格式压缩， 大流量下网络带宽容易成为瓶颈，发送前压缩消息；不影响消费，消费者会自动解压
        pro.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        pro.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return pro;
    }

    public static void main(String[] args) throws JsonProcessingException, InterruptedException {
        long start = System.currentTimeMillis();
        Properties pro = buildProperties();
        String value = "value-";

        @SuppressWarnings("resource")
        Producer<String, String> producer = new KafkaProducer<String, String>(pro);
        for(long index = 0L; index < 1000; index ++){
            Thread.sleep(1000);
            SecureRandom random = new SecureRandom();
            int randomInt = random.nextInt(10);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssmmm");
            Long timestamp = Long.valueOf(sdf.format(System.currentTimeMillis()));
            ChatRecord chatRecord = new ChatRecord("question-" + randomInt, "answer-" + randomInt, timestamp);
            String json = objectMapper.writeValueAsString(chatRecord);
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, "key:" + index, json);
            //底层是异步发送,使用1跟2方法均可以，使用方可以根据实际需求自行选择
            //1. 可以等待获得服务端返回结果
            Future<RecordMetadata> future = producer.send(record);
            try {
                logger.info(json);
                logger.info(future.get().partition() + "-->" + future.get().offset());
            } catch (Exception e) {
                logger.error("send error, details:" + e);
            }
            //2.可以添加回调接口
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception == null){
                        logger.info("{} sends to partion:{}, offset:{}", "key:" + record.key(), metadata.partition(), metadata.offset());
                    }else{
                        logger.error("send error, details:" + exception);
                    }
                }
            });
        }
        producer.close();
        logger.info("takes:{} ms", System.currentTimeMillis() - start);
    }
}
