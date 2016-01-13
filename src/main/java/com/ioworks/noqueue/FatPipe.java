package com.ioworks.noqueue;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA.
 * User: jwang
 * Date: 8/24/12
 * Time: 6:57 PM
 * Producer/Consumer without any queuing.  Data is overwritten
 */
public interface FatPipe<T, S> {

    /**
     *
     * @return returns
     * @throws IllegalArgumentException if a consumer is null
     */
    Receptor<T> takeReceptor();

    /**
     * @param sleep the sleep to wait to prevent the cpu from spinning
     */
    void setSleep(long sleep);
    boolean setConsumers(List<Consumer<S>> consumers, long timeout, TimeUnit unit) throws IllegalArgumentException;
    void shutdown(long time, TimeUnit unit);

    /**
     * Consume data from a producer
     * @param <T> the data to consume
     */
    interface Consumer<T> {

        /**
         * @param data the data to consume
         * @param time the time the data is seen
         */
        void consume(T data, long time);
        boolean isConsuming();
    }

    /**
     * Performs some operation to take input and produce
     * @param <T> The input
     * @param <U> data produced
     */
    interface Producer<T, U> {
        U execute(T data, Receptor<T> r, Signal n);
        void complete(U product);
    }

    interface Signal {
        void signal();

        /**
         * The index of the thread that this is using
         * @return
         */
        int getThreadIndex();

    }

}
