package com.ioworks.noqueue.impl;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ioworks.noqueue.FatPipe;
import com.ioworks.noqueue.FatPipe.Producer;
import com.ioworks.noqueue.ProducerSet;
import com.ioworks.noqueue.ProducerSets;
import com.ioworks.noqueue.Receptor;

public class MapReduceTests {
    static Logger logger = LoggerFactory.getLogger(MapReduceTests.class);
    int producerThreads;
    int poolSize;
    int iterations = 10_000;
    WordCount[] tasks;

    @Before
    public void initialize() {
        ProducerSets.factory = ProducerSetFactoryImpl.class;
        
        // prepare work to do
        WordCount task1 = new WordCount();
        for (int i = 0; i < 1_000; i++) {
            task1.put(UUID.randomUUID().toString().substring(0, 8), (int) (Math.random() * 20));
        }

        WordCount task2 = new WordCount();
        for (int i = 0; i < 1_000; i++) {
            task2.put(UUID.randomUUID().toString().substring(0, 8), (int) (Math.random() * 20));
        }

        WordCount task3 = new WordCount();
        for (int i = 0; i < 1_000; i++) {
            task3.put(UUID.randomUUID().toString().substring(0, 8), (int) (Math.random() * 20));
        }

        WordCount task4 = new WordCount();
        for (int i = 0; i < 1_000; i++) {
            task4.put(UUID.randomUUID().toString().substring(0, 8), (int) (Math.random() * 20));
        }

        tasks = new WordCount[] { task1, task2, task3, task4 };
    }
    
    @Test
    public void multiThreadPerformanceTests() throws InterruptedException, InstantiationException, IllegalAccessException {
        for (int poolSize : new int[] { 1, 4 }) {
            for (int producerThreads : new int[] { 1, 2, 4, 6, 8, 10, 15, 20 }) {
                this.poolSize = poolSize;
                this.producerThreads = producerThreads;
                multiThreadPerformanceTestImpl();
            }
        }
    }

    @Test
    public void pooledFatPipePerformanceTests() throws InterruptedException, InstantiationException, IllegalAccessException {
        for (int poolSize : new int[] { 1, 4 }) {
            for (int producerThreads : new int[] { 1, 2, 4, 6, 8, 10, 15, 20 }) {
                this.poolSize = poolSize;
                this.producerThreads = producerThreads;
                pooledFatPipePerformanceTestImpl(false);
            }
        }
    }

    @Test
    public void pooledFatPipePerformanceReduceContextSwitchTests() throws InterruptedException, InstantiationException, IllegalAccessException {
        for (int poolSize : new int[] { 1, 4 }) {
            for (int producerThreads : new int[] { 1, 2, 4, 6, 8, 10, 15, 20 }) {
                this.poolSize = poolSize;
                this.producerThreads = producerThreads;
                pooledFatPipePerformanceTestImpl(true);
            }
        }
    }
    
    protected void pooledFatPipePerformanceTestImpl(boolean reduceContextSwitch) throws InterruptedException, InstantiationException, IllegalAccessException {
        // initialize producer set
        ReducedConsumer consumer = new ReducedConsumer();
        ProducerSet<String, WordCount, WordCount> set = ProducerSets.newProducerSet(new FatMapReducerGenerator(), 1, 0, null);
        set.addConsumer(consumer, null, 0);
        set.setReduceContextSwitch(reduceContextSwitch);

        // prepare threads for producing work
        ExecutorService producers = Executors.newFixedThreadPool(producerThreads);
        long startTime = System.nanoTime();
        for (int i = 0; i < producerThreads; i++) {
            producers.submit(() -> {
                Receptor<WordCount> receptor = set.get("reducer A");
                for (int r = 0; r < iterations; r++) {
                    // send a task to work on to the receptor
                    WordCount task = new WordCount(tasks[r % tasks.length], System.nanoTime());
                    receptor.lazySet(task);
                    Thread.sleep(2);
                }

                return null;
            });
        }

        producers.shutdown();
        producers.awaitTermination(10, TimeUnit.MINUTES);
        
        // calculate the input throughput
        long endTime = System.nanoTime();
        double inputThroughput = (producerThreads * iterations) / ((endTime - startTime) / 1.0e9);
        
        // shutdown the PooledFatPipe
        set.shutdown();
        logger.info(String.format("%s(%d) output throughput: %.0f, EMA Latency (uS): %.2f, input throughput: %.0f",
                reduceContextSwitch ? "fp-rcs" : "fp", poolSize, consumer.throughput(), consumer.latencyEMA / 1e3,
                inputThroughput));
    }
    
    protected void multiThreadPerformanceTestImpl() throws InterruptedException, InstantiationException, IllegalAccessException {
        // setup single-threaded work queue
        ArrayBlockingQueue<WordCount> workQueue = new ArrayBlockingQueue<>(8192);
        ArrayBlockingQueue<WordCount> outputQueue = new ArrayBlockingQueue<>(8192);
        
        // setup a 'poolSize' daemon threads to do the work
        ReducedConsumer consumer = new ReducedConsumer();
        FatMapReducer reducer = new FatMapReducer();
        ThreadGroup workerGroup = new ThreadGroup("workers");
        for (int p = 0; p < poolSize; p++) {
            Thread worker = new Thread(workerGroup, () -> {
                try {
                    while (true) {
                        WordCount task = workQueue.poll(1, TimeUnit.SECONDS);
                        if (task == null) continue;
                        synchronized (reducer) {
                            WordCount output = reducer.execute(task, null, null);
                            outputQueue.add(output);
                        }
                    }
                } catch (InterruptedException e) {
                    logger.trace("Interrupted", e);
                }
            });
            worker.setDaemon(true);
            worker.start();
        }
        
        // setup a daemon consumer thread
        Thread consumerThread = new Thread(workerGroup, () -> {
            try {
                while (true) {
                    WordCount output = outputQueue.poll(1, TimeUnit.SECONDS);
                    if (output == null) continue;
                    consumer.consume(output, System.nanoTime());
                }
            } catch (InterruptedException e) {
                logger.trace("Interrupted", e);
            }
        });
        consumerThread.setDaemon(true);
        consumerThread.start();
        
        ExecutorService producers = Executors.newFixedThreadPool(producerThreads);
        long startTime = System.nanoTime();
        for (int i = 0; i < producerThreads; i++) {
            producers.submit(() -> {
                for (int r = 0; r < iterations; r++) {
                    // send a task to work on to the work queue
                    WordCount task = new WordCount(tasks[r % tasks.length], System.nanoTime());
                    workQueue.offer(task, 1, TimeUnit.SECONDS);
                    Thread.sleep(1);
                }

                return null;
            });
        }

        producers.shutdown();
        producers.awaitTermination(10, TimeUnit.MINUTES);

        // calculate the input throughput
        long endTime = System.nanoTime();
        double inputThroughput = (producerThreads * iterations) / ((endTime - startTime) / 1.0e9);

        // shutdown the worker threads
        workerGroup.interrupt();
        logger.info(String.format("multi-t(%d) output throughput: %.0f, EMA Latency (uS): %.2f, input throughput: %.0f", poolSize, consumer.throughput(), consumer.latencyEMA / 1e3, inputThroughput));
    }
    
    static class FatMapReducerGenerator implements ProducerSet.Generator<String, WordCount, WordCount> {
        @Override
        public Producer<WordCount, WordCount> generate(String key) {
            return new FatMapReducer();
        }

        @Override
        public void exceptionThrown(Exception e) {
            logger.warn("FatMapReducerGenerator error", e);
        }
    }

    static class FatMapReducer implements FatPipe.Producer<WordCount, WordCount> {
        private final Integer ZERO = Integer.valueOf(0);
        private WordCount runningWordCounts = new WordCount();

        @Override
        public WordCount execute(WordCount data, Receptor<WordCount> r, FatPipe.Signal n) {
            data.forEach((word, count) -> {
                int newCount = runningWordCounts.getOrDefault(word, ZERO) + count;
                runningWordCounts.put(word, newCount);
            });
            return new WordCount(runningWordCounts, data.time);
        }

        @Override
        public void complete(WordCount product) {
        }
    }

    static class ReducedConsumer implements FatPipe.Consumer<WordCount> {
        // start counting throughput after 10,000 invocations
        static final int WARMUP_COUNT_THRESHOLD = 1_000;
        
        int invocations = 0;
        long startTime = 0;
        long lastInvocationTime = 0;
        long lastLogTime = 0;
        double latencyEMA = Double.NaN;

        @Override
        public void consume(WordCount data, long time) {
            ++invocations;
            lastInvocationTime = time;
            
            long latency = System.nanoTime() - data.time;
            if (Double.isNaN(latencyEMA)) latencyEMA = latency;
            else latencyEMA = 0.99 * latencyEMA + 0.01 * latency;
            
            if (startTime == 0 && invocations >= WARMUP_COUNT_THRESHOLD) {
                startTime = time;
            }

            if (TimeUnit.NANOSECONDS.toSeconds(time - lastLogTime) > 1) {
                lastLogTime = time;
                logger.info("Total Words: {}", data.size());
            }
        }
        
        /**
         * Calculates the consumed throughput
         * 
         * @return The consumed throughput in invocations per second, or
         *         Double.NaN if the throughput cannot be calculated.
         */
        public double throughput() {
            if (startTime > 0 && lastInvocationTime > startTime) {
                long elapsedNanos = lastInvocationTime - startTime;
                return (invocations - WARMUP_COUNT_THRESHOLD) / (elapsedNanos / 1.0e9);
            } else {
                return Double.NaN;
            }
        }
        
        public double latencyEMA() {
            return latencyEMA;
        }

        @Override
        public boolean isConsuming() {
            return true;
        }
    }

    @SuppressWarnings("serial")
    static class WordCount extends HashMap<String, Integer> {
        long time;

        public WordCount() {
        }

        public WordCount(WordCount runningWordCounts) {
            super(runningWordCounts);
        }

        public WordCount(WordCount runningWordCounts, long time) {
            super(runningWordCounts);
            this.time = time;
        }
    }
}
