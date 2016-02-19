package com.kimmin.util.queue;

/**
 * Created by min.jin on 2016/2/19.
 */

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by min.jin on 2016/2/1.
 */
public class DiskBackedInMemoryBlockingQueue<T extends Serializable> implements PersistentQueue<T>{

    private BlockingQueue<T> inMemoryQueue;

    // on disk persistent queue
    private PersistentQueue persistentQueue;
    private Object producerLock = new Object();

    private AtomicLong overflowCount = new AtomicLong(0);

    // reload the data on disk into in memeory queue
    private int maxMemoryElementCount;

    /**
     * Constructor
     *
     * @param dir,                   queue directory
     * @param queueName,             the name of the queue
     * @param maxMemoryElementCount, max number of element allowed in the in memory blocking queue.
     * @param maxFileSize,           max size(in MB) of the disk file allowed
     * @throws java.io.IOException
     */

    public DiskBackedInMemoryBlockingQueue(int maxMemoryElementCount, PersistentQueue persistentQueue) throws IOException {
        this.persistentQueue = persistentQueue;
//        this.persistentQueue = new MappedFileQueue(dir, queueName, maxFileSize);
        this.maxMemoryElementCount = maxMemoryElementCount;
        this.inMemoryQueue = new ArrayBlockingQueue<T>(this.maxMemoryElementCount);
//        this.memoryQueueThreshold = maxMemoryElementCount / 2;
//        this.reloader = new Reloader();
//        this.reloader.start();
    }

    @Override
    public boolean produce(T t) {
        boolean success = this.inMemoryQueue.offer(t);
        if (!success) {
            overflowCount.incrementAndGet();
            success = persistentQueue.produce(t);
            if (success) {
                synchronized (producerLock) {
                    producerLock.notify();
                }
            }
        }
        return success;
    }

    @Override
    public T consume() {
        T t = this.inMemoryQueue.poll();
        if (t == null) {
            t = (T) this.persistentQueue.consume();
        }
        return t;
    }

    @Override
    public long getRemainingCapacity() {
        return this.maxMemoryElementCount - inMemoryQueue.size();
    }

    @Override
    public long getTotalCapacity() {
        return this.maxMemoryElementCount;
    }

    @Override
    public long getUsedCapacity() {
        return inMemoryQueue.size();
    }

    @Override
    public boolean isEmpty() {
        return this.inMemoryQueue.isEmpty() && this.persistentQueue.isEmpty();
    }


    @Override
    public void shutdown() {

        this.persistentQueue.shutdown();

    }

    @Override
    public long getOverflowCount() {
        return overflowCount.get();
    }

    @Override
    public int getBackFileSize() {
        return this.persistentQueue.getBackFileSize();
    }


}
