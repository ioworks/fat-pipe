package com.ioworks.noqueue;

import java.util.concurrent.TimeUnit;

/**
 * Receive input used by the producer
 * @param <T> the input the producer
 */
public interface Receptor<T> {
    /**
     * @param data
     * @return true if this results in skipped data 
     */
    boolean set(T data);

    /**
     *
     * @param data the data to set
     * @return true if that data was able to be set
     */
    boolean setIfEmpty(T data);

    void lazySet(T data);

    /**
     * set the data within a certain amount of time
     * @param data
     * @param time
     * @param units
     * @return
     */
    boolean setIfEmpty(T data, long time, TimeUnit units);
    int skipCount();

    /**
     * return this as it is no longer used
     */
    void dispose();

}
