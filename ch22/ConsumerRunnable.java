package ch22;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
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
    KafkaConsumer<String, String> m_consumer;
    // 用来提交偏移量
    private BlockingQueue<Map<TopicPartition, OffsetAndMetadata>> m_commitQueue = new LinkedBlockingQueue<>();
    private Properties consumerConfig;
    private String m_topic;
    // 存放每个分区的Worker Runnable
    private ConcurrentHashMap<TopicPartition, Worker> workers;
    // 存放处理每个分区的线程
    private ConcurrentHashMap<TopicPartition, Thread> workerThreads;
    private CountDownLatch m_latch;

    ConsumerRunnable(Properties consumerConfig, String topic,
                     ConcurrentHashMap<TopicPartition, Worker> workers,
                     ConcurrentHashMap<TopicPartition, Thread> workerThreads, CountDownLatch latch) {
        this.consumerConfig = consumerConfig;
        m_topic = topic;
        this.workers = workers;
        this.workerThreads = workerThreads;
        this.m_latch = latch;
    }

    @Override
    public void run() {
        //kafkaConsumer是非线程安全的,所以需要每个线程建立一个consumer
        m_consumer = new KafkaConsumer<>(consumerConfig);
        m_consumer.subscribe(Collections.singleton(m_topic));
        TopicPartition topicPartition = null;
        //检查线程中断标志是否设置, 如果设置则表示外界想要停止该任务,终止该任务
        try {
            while (true) {
                //查看该消费者是否有需要提交的偏移信息, 使用非阻塞读取 Retrieves and removes the head of this queue
                Map<TopicPartition, OffsetAndMetadata> toCommit = m_commitQueue.poll();
                if (toCommit != null) {
                    logger.info("提交 offset to TopicPartition: " + toCommit);
                    m_consumer.commitSync(toCommit);
                }
                // 由这个线程的KafkaConsumer实例执行轮询
                ConsumerRecords<String, String> records = m_consumer.poll(Duration.ofMillis(100));
                for (final ConsumerRecord<String, String> record : records) {
                    String topic = record.topic();
                    int partition = record.partition();
                    topicPartition = new TopicPartition(topic, partition);

                    Worker worker = workers.get(topicPartition);
                    //如果当前分区还没有开始消费, 则就没有消费任务在map中
                    if (worker == null) {
                        // 生成新的处理任务和线程, 然后将其放入对应的map中进行保存
                        worker = new Worker(m_commitQueue);
                        workers.put(topicPartition, worker);
                        Thread thread = new Thread(worker);
                        thread.setName("worker-thread-" + topic + "-" + partition);
                        thread.start();
                        workerThreads.put(topicPartition, thread);
                    }
                    // 将消息添加到工作线程的数据队列中
                    worker.addRecordToQueue(record);
                }
            }
        } catch (WakeupException ex) {
            logger.info("The consumer thread " + Thread.currentThread().getName() + " is shutdown...");
            // 停掉对应的工作线程
            for (TopicPartition tp: m_consumer.assignment()) {
                Thread wt = workerThreads.get(tp);
                if (wt != null) {
                    wt.interrupt();
                }
            }
        } finally {
            // 让工作线程先进行最后提交 offset 再运行下面的代码
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // 希望工作线程已经完成提交 offset 到 commitQueue。现在由 consumer 线程提交到 coordinator
            Map<TopicPartition, OffsetAndMetadata> toCommit = m_commitQueue.poll();
            if (toCommit != null) {
                logger.info("退出之前提交 offset to TopicPartition: " + toCommit);
                m_consumer.commitSync(toCommit);
            }
            m_consumer.close();
            // 通知主线程 m_consumer已经关闭。倒计时闩锁的计数，如果计数达到零，则释放所有等待m_latch的线程。
            m_latch.countDown();
        }
    }

    public void shutdown() {
        // wakeup() 是 KafkaConsumer中唯一可以从不同线程中安全调用的方法。
        // 从shutdown hook线程调用 KafkaConsumer.wakeup()方法。使 m_consumer.poll()抛出
        // WakeUpException，而中止长轮询。
        // 注意：m_consumer实例被两个线程使用。一个是shutdown hook线程。另一个是KafkaConsumer线程
        m_consumer.wakeup();
    }
}
