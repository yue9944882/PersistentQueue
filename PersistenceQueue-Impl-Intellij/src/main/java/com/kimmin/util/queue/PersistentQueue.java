package com.kimmin.util.queue;

/**
 * Created by min.jin on 2016/2/19.
 */

import java.io.Serializable;


import java.io.Serializable;
import java.util.Queue;

/**
 * Created by min.jin on 2016/1/29.
 */
public interface PersistentQueue< T extends Serializable>{

    //Primitive Action :
    boolean produce(T t);
    T consume();

    //Query Entry Point
    long getRemainingCapacity();
    long getUsedCapacity();
    long getOverflowCount();
    long getTotalCapacity();

    /**
     * Get Back File Size in MB
     *
     * @Reutrn back file size integer value
     * **/
    int getBackFileSize();
    boolean isEmpty();


    //Manipulation Entry Point
    void shutdown();


}
