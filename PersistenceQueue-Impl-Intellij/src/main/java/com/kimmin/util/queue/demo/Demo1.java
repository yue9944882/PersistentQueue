package com.kimmin.util.queue.demo;

/**
 * Created by min.jin on 2016/2/19.
 */

import com.kimmin.util.queue.PersistentQueue;
import com.kimmin.util.queue.DiskBackedInMemoryBlockingQueue;
import com.kimmin.util.queue.MappedFileQueue;

import java.io.IOException;

/** A Simple Demo of DiskBackedInMemoryBlockingQueue **/
public class Demo1 {

    public static void main(String[] args) throws IOException{

        PersistentQueue<Integer> persistentQueue = new DiskBackedInMemoryBlockingQueue<Integer>(100,new MappedFileQueue("D:\\pq_buffer1","demo1_queue",32*1024));// 32*1024 Mb

        for(int i=0;i<100000000;i++)
            persistentQueue.produce(i);

        Integer i = persistentQueue.consume();

        System.out.println("[GET]:\t"+i);

        persistentQueue.shutdown();

    }

}
