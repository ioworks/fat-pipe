package com.ioworks.noqueue.impl;

import org.slf4j.Logger;

import com.ioworks.noqueue.ProducerSet;
import com.ioworks.noqueue.ProducerSetFactory;

/**
* Created by jwang on 3/19/15.
*/
public class ProducerSetFactoryImpl implements ProducerSetFactory {

    @Override
    public <K, T, U> ProducerSet<K, T, U> createProducerSet(ProducerSet.Generator<K, T, U> generator, int threadPool, long sleep, Logger logger) {
        return new ProducerSetImpl<>(threadPool, generator, sleep);
    }
}
