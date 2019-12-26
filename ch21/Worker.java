package ch21;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Worker implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(Worker.class);

    // 用于向consumer线程提交offset的队列
    private BlockingQueue<Map<TopicPartition, OffsetAndMetadata>> commitQueue;
    //保存consumer线程发送过来的消息
    private BlockingQueue<ConsumerRecord<String, String>> dataQueue = new LinkedBlockingQueue<>();
    // 上一次提交时间
    private LocalDateTime lastTime = LocalDateTime.now();
    // 消费了20条数据, 就进行一次提交
    private Integer commitLength = 20;
    // 距离上一次提交多久, 就提交一次
    private Duration commitDuration = Duration.ofSeconds(2);
    // 当前线程消费的数据条数
    private Long completedRecords = 0L;
    // 保存上一条消费的数据
    private ConsumerRecord<String, String> lastUncommittedRecord = null;

    // 用于保存offset的队列, 由ConsumerRunnable提供
    public Worker(BlockingQueue<Map<TopicPartition, OffsetAndMetadata>> commitQueue) {
        this.commitQueue = commitQueue;
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                // 每个工作线程都有自己的数据队列
                ConsumerRecord<String, String> record = dataQueue.poll(100, TimeUnit.MICROSECONDS);
                if (record != null) { // 如果队列有数据
                    // 处理数据
                    process(record);
                    // 完成记录数加1
                    this.completedRecords++;
                    // 保存上一条处理记录
                    lastUncommittedRecord = record;
                }
                // 提交偏移给consumer
                commitToQueue();
            }
        } catch (InterruptedException e) {
            // 线程被interrupted,直接退出
            logger.info(Thread.currentThread() + "is interrupted");
        }
    }

    /**
     * 处理一条记录
     * @param record
     */
    private void process(ConsumerRecord<String, String> record) {
        System.out.println("partition = " + record.partition() + ", offset = " + record.offset());
    }

    /**
     * 将当前的消费偏移量放到commitQueue中, 由consumer线程提交
     * @throws InterruptedException
     */
    private void commitToQueue() throws InterruptedException {
        if (lastUncommittedRecord == null) return;
        // 如果消费了设定的条数, 比如又消费了commitLength消息
        boolean arrivedCommitLength = this.completedRecords % commitLength == 0;
        // 获取当前时间, 看是否已经到了需要提交的时间
        LocalDateTime currentTime = LocalDateTime.now();
        boolean arrivedTime = currentTime.isAfter(lastTime.plus(commitDuration));
        //如果消费了设定条数, 或者到了设定时间, 那么就发送偏移到消费者, 由消费者非阻塞poll这个偏移量信息队列, 进行提交
        if (arrivedCommitLength || arrivedTime) {
            lastTime = currentTime;
            Long offset = lastUncommittedRecord.offset();
            Integer partition = lastUncommittedRecord.partition();
            String topic = lastUncommittedRecord.topic();
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            logger.debug("partition: " + topicPartition + " submit offset: " + (offset + 1L) + " to consumer task");
            Map<TopicPartition, OffsetAndMetadata> map = Collections.singletonMap(topicPartition, new OffsetAndMetadata(offset + 1L));
            commitQueue.put(map);
            // 置空
            lastUncommittedRecord = null;
        }
    }

    // 由KafkaConsumer线程调用，向工作线程的数据队列添加record
    public void addRecordToQueue(ConsumerRecord<String, String> record) {
        try {
            dataQueue.put(record);
        } catch (InterruptedException e) {
            logger.debug("interrupted exception thrown " + e.getMessage());
        }
    }
}
