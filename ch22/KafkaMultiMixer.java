package ch22;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class KafkaMultiMixer {
    // 整个进程所有的Worker Runnable 与主题分区的映射
    private ConcurrentHashMap<TopicPartition, Worker> workers = new ConcurrentHashMap<>();
    // 整个进程所有的工作线程 与主题分区的映射
    private ConcurrentHashMap<TopicPartition, Thread> workerThreads = new ConcurrentHashMap<>();
    // 用来存放程序中的各KafkaConsumer线程
    private ConsumerRunnable[] consumerRunnables;

    public static void main(String[] args) {
        KafkaMultiMixer app = new KafkaMultiMixer();
        //创建threadsNum个线程用于读取kafka消息, 且位于同一个group中, 这个topic有12个分区, 最多12个consumer进行读取
        app.execute(3);
    }

    private void execute(int numOfThreads) {
        Properties consumerConfig = getConsumerConfig();
        consumerRunnables = new ConsumerRunnable[numOfThreads];
        CountDownLatch latch = new CountDownLatch(1);

        for (int i = 0; i < numOfThreads; i++) {
            ConsumerRunnable consumerRunnable = new ConsumerRunnable(
                    consumerConfig, "x-topic", workers, workerThreads, latch);
            consumerRunnables[i] = consumerRunnable;
            Thread thread = new Thread(consumerRunnable);
            thread.setName("consumer-thread-" + i);
            // 启动KafkaConsumer线程
            thread.start();
        }

        // 添加shutdown hook。shutdown hook运行在不同于主线程的线程中。
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Thread.currentThread().setName("shutdown-hook-thread");
            System.out.println("从线程 " + Thread.currentThread().getName() + " 捕捉到程序退出的信号。");
            for (int i = 0; i < numOfThreads; i++)
                consumerRunnables[i].shutdown();
            try {
                // 使当前线程(shutdown hook)等待latch倒计时为零
                latch.await();
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }
        }
        ));
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
        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "30000");


        return properties;
    }
}