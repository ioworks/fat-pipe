package com.ioworks.noqueue;

import org.slf4j.Logger;

/**
 * Created by jwang on 3/19/15.
 */
public class ProducerSets {

    public static Class<? extends ProducerSetFactory> factory;

    public static <K, T, U> ProducerSet<K, T, U> newProducerSet(ProducerSet.Generator<K, T, U> generator, int threadPool, long sleep, Logger logger) throws IllegalAccessException, InstantiationException {
        return factory.newInstance().createProducerSet(generator, threadPool, sleep, logger);
    }

}
