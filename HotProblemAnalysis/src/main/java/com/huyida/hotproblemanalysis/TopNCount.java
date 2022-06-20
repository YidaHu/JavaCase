package com.huyida.hotproblemanalysis;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.huyida.hotproblemanalysis.domain.ChatRecord;
import com.huyida.hotproblemanalysis.domain.ItemViewCount;
import com.huyida.hotproblemanalysis.test.KafkaConsumerTest;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Properties;

/**
 * @program: HotProblemAnalysis
 * @description:
 * @author: huyida
 * @create: 2022-06-20 15:18
 **/

public class TopNCount {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerTest.class);
    private static final String KAFKA_SERVER = "127.0.0.1:9092";
    private static final String Topic = "topic-chat-record";
    private static final String ConsumerGroup = "consumer-test-qa";

    //最对延迟到达的时间
    public static final long MAX_EVENT_DELAY = 10L;

    private static final ObjectMapper objectMapper = new ObjectMapper();

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

    /**
     * main() defines and executes the DataStream program.
     *
     * @param args program arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // use event time for the application
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // configure watermark interval
        env.getConfig().setAutoWatermarkInterval(1000L);

        final Properties properties = buildProperties();
        // Source
        FlinkKafkaConsumer011<String> consumer =
                new FlinkKafkaConsumer011<String>(Topic, new SimpleStringSchema(), properties);
        DataStream<String> stream = env.addSource(consumer);

        //从kafka里读取数据，转换成UserAction对象
        DataStream<ChatRecord> dataStream = stream.map(value -> objectMapper.readValue(value, ChatRecord.class));

        //将乱序的数据进行抽取出来，设置watermark，数据如果晚到10秒的会被丢弃
        DataStream<ChatRecord> timedData = dataStream.assignTimestampsAndWatermarks(new UserActionTSExtractor());

        //为了统计5分钟购买的最多的，所以我们需要过滤出购买的行为
        DataStream<ChatRecord> filterData = timedData.filter(new FilterFunction<ChatRecord>() {
            @Override
            public boolean filter(ChatRecord userAction) throws Exception {
                return userAction.getAnswer().contains("answer");
            }
        });

        //窗口统计点击量 滑动的窗口 5分钟一次  统计一小时最高的  比如 [09:00, 10:00), [09:05, 10:05), [09:10, 10:10)…
        DataStream<ItemViewCount> windowedData = filterData
                .keyBy("answer")
                .timeWindow(Time.minutes(60L), Time.minutes(1L))
                .aggregate(new CountAgg(), new WindowResultFunciton());


        //Top N 计算最热门的商品
//        DataStream<List<ItemViewCount>> topItems = windowedData
//                .keyBy("windowEnd")
//                .process(new TopNHotItems(3));

        DataStream<String> resultStream = windowedData
                .keyBy("windowEnd")    // 按照窗口分组
                .process(new TopNHotItems(5));   // 用自定义处理函数排序取前5

//        topItems.addSink(new MySqlSink());
        //topItems.print();
        resultStream.print();
        env.execute("Top N Job");
    }

    /**
     * 用于行为时间戳抽取器，最多十秒延迟，也就是晚到10秒的数据会被丢弃掉
     */
    public static class UserActionTSExtractor extends BoundedOutOfOrdernessTimestampExtractor<ChatRecord> {


        public UserActionTSExtractor() {
            super(Time.seconds(MAX_EVENT_DELAY));
        }

        @Override
        public long extractTimestamp(ChatRecord userAction) {
            return userAction.getTimestamp();
        }
    }

    /**
     * COUNT 聚合函数实现，每出现一条记录加一。AggregateFunction<输入，汇总，输出>
     */
    public static class CountAgg implements AggregateFunction<ChatRecord, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ChatRecord userAction, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1 + acc2;
        }
    }

    /**
     * 用于输出结果的窗口WindowFunction<输入，输出，键，窗口>
     */
    public static class WindowResultFunciton implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

        @Override
        public void apply(
                Tuple key, //窗口主键即itemId
                TimeWindow window, //窗口
                Iterable<Long> aggregationResult, //集合函数的结果，即count的值
                Collector<ItemViewCount> collector //输出类型collector
        ) throws Exception {

            String itemId = ((Tuple1<String>) key).f0;
            Long count = aggregationResult.iterator().next();
            collector.collect(ItemViewCount.of(itemId, window.getEnd(), count));

        }
    }

    // 实现自定义KeyedProcessFunction
    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {
        // 定义属性，top n的大小
        private Integer topSize;

        public TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }

        // 定义列表状态，保存当前窗口内所有输出的ItemViewCount
        ListState<ItemViewCount> itemViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-view-count-list", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据，存入List中，并注册定时器
            itemViewCountListState.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，当前已收集到所有数据，排序输出
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());

            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.getCount().intValue() - o1.getCount().intValue();
                }
            });

            // 将排名信息格式化成String，方便打印输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("===================================\n");
            resultBuilder.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");

            // 遍历列表，取top n输出
            for (int i = 0; i < Math.min(topSize, itemViewCounts.size()); i++) {
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                resultBuilder.append("NO ").append(i + 1).append(":")
                        .append(" 问题 = ").append(currentItemViewCount.getItemId())
                        .append(" 热门度 = ").append(currentItemViewCount.getCount())
                        .append("\n");
            }
            resultBuilder.append("===============================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());
        }
    }

}
