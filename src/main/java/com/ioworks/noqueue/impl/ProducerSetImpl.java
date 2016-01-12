package com.ioworks.noqueue.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ioworks.noqueue.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA.
 * User: jwang
 * Date: 9/6/12
 * Time: 10:32 PM
 * mapping of serveral producers
 */
public class ProducerSetImpl<K, T, U> implements ProducerSet<K, T, U> {

    protected Logger logger = LoggerFactory.getLogger("com.ioworks.noqueue.ProducerSetImpl");

    private PooledFatPipe.Pool<U> pool;
    private Map<K, PooledFatPipe<T, U>> pipes;
    private Generator<K,T,U> generator;
    private List<FatPipe.Consumer<U>> consumers;
    private boolean reduceContextSwitch;

    public ProducerSetImpl(int poolSize, Generator<K, T, U> generator, long sleep) {
        this.generator = generator;
        pool = new PooledFatPipe.Pool<>(poolSize, generator.getClass().getSimpleName(), generator);
        setSleep(sleep);
        pipes = new ConcurrentHashMap<>();
        consumers = new ArrayList<>();
        pool.start();
    }

    /**
     * @param maxTime The maximum milliseconds allowed
     * @return The consumer that exceeds the threashold, otherwise null
     */
    public FatPipe.Consumer<U> check(long maxTime) {
        return pool.check(maxTime);
    }

    public synchronized List<FatPipe.Producer<T, U>> getProducers() {
        List<FatPipe.Producer<T, U>> list = new ArrayList<>();
        for(PooledFatPipe<T, U> pipe : pipes.values()) {
            list.add(pipe.getProducer());
        }
        return list;
    }

    @Override
    public void setSleep(long sleep) {
        pool.setSleep(sleep);
    }

    /**
     * get a new receptor from the producer.  If the producer does not exist, create a new one
     * @param key for the Producer
     * @return a newly allocated receptor
     */
    @Override
    public synchronized Receptor<T> get(K key) {
        PooledFatPipe<T, U> pipe = pipes.get(key);
        if(pipe!=null) return pipe.takeReceptor();
        FatPipe.Producer<T, U> producer = generator.generate(key);
        if(producer==null) throw new IllegalArgumentException("unable to generate "+key);
        pipe = new PooledFatPipe<>(producer, pool, generator);
        pipe.start();
        pipe.setReduceContextSwitch(reduceContextSwitch);
        pipes.put(key, pipe);
        if(consumers!=null) pipe.setConsumers(consumers, 1, TimeUnit.SECONDS);
        return pipe.takeReceptor();
    }

    /**
     * @param consumers the consumers to set
     * @param waitTime the maximum time to wait for changing the consumers
     */
    void setConsumers(List<FatPipe.Consumer<U>> consumers, long waitTime) {
        this.consumers = new ArrayList<>(consumers);
        for(PooledFatPipe<T, U> pipe : pipes.values()) {
            pipe.setConsumers(this.consumers, waitTime, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public synchronized boolean addConsumer(FatPipe.Consumer<U> consumer, FatPipe.Consumer<U> afterConsumer, long waitTime) {
        if(afterConsumer==null) {
            consumers.add(consumer);
            setConsumers(consumers, waitTime);
            return true;
        }
        for(int i=0; i<consumers.size(); i++) {
            FatPipe.Consumer<U> current = consumers.get(i);
            if(current!=afterConsumer) continue;
            consumers.add(i+1, consumer);
            setConsumers(consumers, waitTime);
            return true;
        }
        logger.warn("unable to add {} after {}: {}", consumer, afterConsumer, consumers);
        return false;
    }

    @Override
    public synchronized boolean removeConsumers(FatPipe.Consumer<U>[] consumer, long waitTime) {
        if(consumer==null || consumer.length==0) return true;
        boolean reset = false;
        for(int j=0; j<consumer.length; j++) {
            for(Iterator<FatPipe.Consumer<U>> i=consumers.iterator(); i.hasNext();) {
                if(consumer[j]==i.next()) {
                    reset = true;
                    i.remove();
                    break;
                }
            }
        }
        if(reset) setConsumers(consumers, waitTime);
        return reset;
    }

    @Override
    public void setReduceContextSwitch(boolean value) {
        reduceContextSwitch = value;
        for(PooledFatPipe<T, U> pipe : pipes.values()) {
            pipe.setReduceContextSwitch(value);
        }
    }

    @Override
    public PipeLocal getLocal() {
        return new PipeLocalImpl<>(pool.parent.threads);
    }

    @Override
    public void shutdown() {
        logger.info("consumers {}", consumers);
        pool.shutdown(5, TimeUnit.SECONDS);
    }
}
