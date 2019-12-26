package ch20;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MultiWorkerApp {
    private static Logger logger = LoggerFactory.getLogger(MultiWorkerApp.class);
    private static String brokerList = "localhost:9092,localhost:9093,localhost:9094";
    private static String groupId = "polar-bear-app";
    private static String[] topics = {"x-topic"};

    private KafkaConsumer<String, String> m_consumer;
    private ExecutorService executors;
    // 存放处理每个分区的Worker Runnable
    private Map<TopicPartition, Worker> workers = new HashMap<>();

    public static void main(String[] args) {
        // KafkaConsumer 拉取的实例交由工作线程去完成。
        new MultiWorkerApp().execute(1);
    }

    public void execute(int workerNum) {
        Properties properties = getConsumerConfig(brokerList, groupId);
        // 应用程序中只有一个KafkaConsumer实例
        m_consumer = new KafkaConsumer<>(properties);
        m_consumer.subscribe(Arrays.asList(topics));

        executors = new ThreadPoolExecutor(workerNum, workerNum, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

        while (true) {
            ConsumerRecords<String, String> records = m_consumer.poll(Duration.ofMillis(200));
            for (final ConsumerRecord record : records) {
                String topic = record.topic();
                int partition = record.partition();
                TopicPartition topicPartition = new TopicPartition(topic, partition);
                Worker worker = workers.get(topicPartition);
                if (worker == null) {
                    worker = new Worker();
                    workers.put(topicPartition, worker);
                }
                //将消息添加到worker的数据队列中
                worker.addRecordToQueue(record);
                // 把KafkaConsumer实例poll回来的数据交由工作线程(池)去处理
                executors.submit(worker);
            }
        }
    }


    //KafkaConsumer配置
    private Properties getConsumerConfig(String brokerList, String groupId) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return properties;
    }
}