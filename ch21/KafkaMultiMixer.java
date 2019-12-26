package ch21;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaMultiMixer {
    // 整个进程所有的Worker Runnable 与主题分区的映射
    private ConcurrentHashMap<TopicPartition, Worker> workers = new ConcurrentHashMap<>();
    // 整个进程所有的工作线程 与主题分区的映射
    private ConcurrentHashMap<TopicPartition, Thread> workerThreads = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        KafkaMultiMixer app = new KafkaMultiMixer();
        app.execute();
    }

    private void execute() {
        Properties consumerConfig = getConsumerConfig();
        int numOfConsumers = 3;
        for (int i = 0; i < numOfConsumers; i++) {
            ConsumerRunnable consumerRunnable = new ConsumerRunnable(
                    consumerConfig, "x-topic", workers, workerThreads);
            Thread consumerThread = new Thread(consumerRunnable);
            consumerThread.start();
        }
    }

    // 配置KafkaConsumer
    private Properties getConsumerConfig() {
        Properties properties = new Properties();
        String brokerList = "localhost:9092,localhost:9093,localhost:9094";
        String groupId = "multi-thread-app";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return properties;
    }
}