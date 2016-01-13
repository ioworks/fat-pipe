package com.ioworks.noqueue;

import org.slf4j.Logger;

/**
 * Created by jwang on 3/19/15.
 */
public interface ProducerSetFactory {
    <K, T, U> ProducerSet<K, T, U> createProducerSet(ProducerSet.Generator<K, T, U> generator, int threadPool, long sleep, Logger logger);
}
