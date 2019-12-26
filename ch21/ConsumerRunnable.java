package ch21;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *    代表一个有KafkaConsumer实例的一个线程。
 *   每个KafkaConsumer都有一个commitQueue,用于存放这个consumer已经消费过的记录offset
 *   由里面的KafkaConsumer do poll；为工作线程备料；手工commit offset;
 *   KafkaConsumer所在的线程本身不处理消息记录
 *  把KafkaConsumer poll到的消息记录分发到对应的工作线程。
 *
 *
 *
 */
public class ConsumerRunnable implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
    // 用来提交偏移量
    private BlockingQueue<Map<TopicPartition, OffsetAndMetadata>> m_commitQueue = new LinkedBlockingQueue<>();
    private Properties consumerConfig;
    private String m_topic;
    // 存放每个分区的Worker Runnable
    private ConcurrentHashMap<TopicPartition, Worker> workers;
    // 存放处理每个分区的线程
    private ConcurrentHashMap<TopicPartition, Thread> workerThreads;

    ConsumerRunnable(Properties consumerConfig, String topic,
                     ConcurrentHashMap<TopicPartition, Worker> workers,
                     ConcurrentHashMap<TopicPartition, Thread> workerThreads) {
        this.consumerConfig = consumerConfig;
        m_topic = topic;
        this.workers = workers;
        this.workerThreads = workerThreads;
    }

    @Override
    public void run() {
        // 每个consumer线程创建一个KafkaConsumer实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig);
        consumer.subscribe(Collections.singleton(m_topic));

        while (true) {
            // 查看该消费者是否有需要提交的偏移信息
            Map<TopicPartition, OffsetAndMetadata> toCommit = m_commitQueue.poll();
            if (toCommit != null) {
                consumer.commitSync(toCommit);
            }
            // 由这个线程的KafkaConsumer实例执行轮询
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (final ConsumerRecord<String, String> record : records) {
                String topic = record.topic();
                int partition = record.partition();
                TopicPartition topicPartition = new TopicPartition(topic, partition);

                Worker worker = workers.get(topicPartition);
                //如果当前分区还没有开始消费, 则就没有消费任务在map中
                if (worker == null) {
                    // 生成新的处理任务和线程, 然后将其放入对应的map中进行保存
                    worker = new Worker(m_commitQueue);
                    workers.put(topicPartition, worker);
                    Thread thread = new Thread(worker);
                    thread.start();
                    workerThreads.put(topicPartition, thread);
                }
                // 将消息添加到工作线程的数据队列中
                worker.addRecordToQueue(record);
            }
        }
    }
}
