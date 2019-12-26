package ch20;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Worker implements Runnable {

    // 保存KafkaConsumer实例从主线程分发过来的消息
    private BlockingQueue<ConsumerRecord<String, String>> dataQueue = new LinkedBlockingQueue<>();

    @Override
    public void run() {
        // 消息处理逻辑
        try {
            ConsumerRecord<String, String> record = dataQueue.poll(50, TimeUnit.MICROSECONDS);
                System.out.println("partition = " + record.partition() + ", offset = " + record.offset());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void addRecordToQueue(ConsumerRecord record) {
        dataQueue.add(record);
    }
}