package com.kimmin.util.queue.demo;

import com.kimmin.util.queue.ByteArrayPersistentQueue;
import com.kimmin.util.queue.PersistentQueue;

import java.io.CharArrayReader;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Created by min.jin on 2016/2/19.
 */
public class Demo2 {

    public static void main(String[] args) throws IOException{

        PersistentQueue<byte[]> persistentQueue = new ByteArrayPersistentQueue("D:\\pq_buffer2","demo2_queue",32*1024);

        persistentQueue.produce("Hello,Queue".getBytes());

        String str = new String(persistentQueue.consume(), Charset.defaultCharset());

        System.out.println("[GET]:\t"+str);

        persistentQueue.shutdown();

    }

}
