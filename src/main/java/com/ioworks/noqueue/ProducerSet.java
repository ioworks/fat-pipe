package com.ioworks.noqueue;

import java.beans.ExceptionListener;

/**
 * Created by jwang on 3/19/15.
 * @param <K> The key for which Producer will be used.
 * @param <T> The type of object the producer will be able to handle
 * @param <U> The type of object that is produced
 */
public interface ProducerSet<K, T, U> {

    interface Generator<K, T, U> extends ExceptionListener {
        FatPipe.Producer<T, U> generate(K key);
    }

    void setSleep(long sleep);

    Receptor<T> get(K key);

    /**
     * @param consumer The consumer to add
     * @param afterConsumer Add the consumer directly after this consumer.  If not set, then add to the end
     * @param waitTime The longest time to wait to add in milliseconds
     * @return true if added successfully
     */
    boolean addConsumer(FatPipe.Consumer<U> consumer, FatPipe.Consumer<U> afterConsumer, long waitTime);

    boolean removeConsumers(FatPipe.Consumer<U>[] consumers, long waitTime);

    /**
     * @param value if true Receptor#set will be combined with the execution
     */
    void setReduceContextSwitch(boolean value);

    /**
     * T
     * @return
     */
    PipeLocal getLocal();

    void shutdown();

}
