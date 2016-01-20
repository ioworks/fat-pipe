package com.ioworks.noqueue.impl;

import java.util.HashMap;
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
    int threads = 4;
    int poolSize = 3;
    int iterations = 20_000;
    WordCount[] tasks;
    ReducedConsumer consumer;

    @Before
    public void initialize() {
        ProducerSets.factory = ProducerSetFactoryImpl.class;
        
        // prepare work to do
        WordCount task1 = new WordCount();
        task1.put("Hello", 1);
        task1.put("World", 1);
        task1.put("Apache", 1);

        WordCount task2 = new WordCount();
        task2.put("The", 1);
        task2.put("Whole", 1);
        task2.put("World", 1);

        tasks = new WordCount[] { task1, task2 };
        
        // consumer will receive the work output
        consumer = new ReducedConsumer();
    }

    @Test
    public void pooledFatPipePerformanceTest() throws InterruptedException, InstantiationException, IllegalAccessException {
        // initialize producer set
        ProducerSet<String, WordCount, WordCount> set = ProducerSets.newProducerSet(new FatMapReducerGenerator(), 1, 0, null);
        set.addConsumer(consumer, null, 0);

        ExecutorService producers = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            producers.submit(() -> {
                Receptor<WordCount> receptor = set.get("reducer A");
                for (int r = 0; r < iterations; r++) {
                    // send a task to work on to the receptor
                    WordCount task = tasks[r % tasks.length];
                    receptor.lazySet(task);
                    Thread.sleep(1);
                }

                return null;
            });
        }

        producers.shutdown();
        producers.awaitTermination(10, TimeUnit.SECONDS);
        TimeUnit.SECONDS.sleep(5);
        set.shutdown();
        logger.info("PooledFatPipe throughput (maps per second): {}", consumer.throughput());
    }

    @Test
    public void singleThreadedPerformanceTest() throws InterruptedException, InstantiationException, IllegalAccessException {
        // setup single-threaded work queue
        ArrayBlockingQueue<WordCount> workQueue = new ArrayBlockingQueue<>(1024*1024);
        
        // setup a daemon thread to do the work
        Thread worker = new Thread() {
            FatMapReducer reducer = new FatMapReducer();

            @Override
            public void run() {
                while (true) {
                    try {
                        WordCount task;
                        task = workQueue.poll(1, TimeUnit.SECONDS);
                        if (task == null) continue;
                        WordCount output = reducer.execute(task, null, null);
                        consumer.consume(output, System.nanoTime());
                    } catch (InterruptedException e) {
                        logger.trace("Interrupted", e);
                    }
                }
            }
        };
        worker.setDaemon(true);
        worker.start();
        
        ExecutorService producers = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            producers.submit(() -> {
                for (int r = 0; r < iterations; r++) {
                    // send a task to work on to the work queue
                    WordCount task = tasks[r % tasks.length];
                    workQueue.offer(task, 1, TimeUnit.SECONDS);
                    Thread.sleep(1);
                }

                return null;
            });
        }

        producers.shutdown();
        producers.awaitTermination(10, TimeUnit.SECONDS);
        TimeUnit.SECONDS.sleep(5);
        worker.interrupt();
        logger.info("Single thread throughput (maps per second): {}", consumer.throughput());
    }
    
    @Test
    public void multiThreadPerformanceTest() throws InterruptedException, InstantiationException, IllegalAccessException {
        // setup single-threaded work queue
        ArrayBlockingQueue<WordCount> workQueue = new ArrayBlockingQueue<>(1024*1024);
        
        // setup a daemon thread to do the work
        ThreadGroup workerGroup = new ThreadGroup("workers");
        for (int p=0; p<poolSize; p++) {
            Thread worker = new Thread(workerGroup, new Runnable() {
                FatMapReducer reducer = new FatMapReducer();
    
                @Override
                public void run() {
                    while (true) {
                        try {
                            WordCount task;
                            task = workQueue.poll(1, TimeUnit.SECONDS);
                            if (task == null) continue;
                            WordCount output = reducer.execute(task, null, null);
                            consumer.consume(output, System.nanoTime());
                        } catch (InterruptedException e) {
                            logger.trace("Interrupted", e);
                        }
                    }
                }
            });
            worker.setDaemon(true);
            worker.start();
        }
        
        ExecutorService producers = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            producers.submit(() -> {
                for (int r = 0; r < iterations; r++) {
                    // send a task to work on to the work queue
                    WordCount task = tasks[r % tasks.length];
                    workQueue.offer(task, 1, TimeUnit.SECONDS);
                    Thread.sleep(1);
                }

                return null;
            });
        }

        producers.shutdown();
        producers.awaitTermination(10, TimeUnit.SECONDS);
        TimeUnit.SECONDS.sleep(5);
        workerGroup.interrupt();
        logger.info("Multi thread throughput (maps per second): {}", consumer.throughput());
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
                
                // simulate load
                for (int i=0; i<100_000; i++) {
                    double x = 12345.0 * 54321.0;
                }
            });
            return new WordCount(runningWordCounts);
        }

        @Override
        public void complete(WordCount product) {
        }
    }

    static class ReducedConsumer implements FatPipe.Consumer<WordCount> {
        // start counting throughput after 10,000 invocations
        static final int WARMUP_COUNT_THRESHOLD = 10_000;
        int invocations = 0;
        
        long startTime = 0;
        long lastInvocationTime = 0;
        long lastLogTime = 0;

        @Override
        public void consume(WordCount data, long time) {
            ++invocations;
            lastInvocationTime = time;
            
            if (startTime == 0 && invocations > WARMUP_COUNT_THRESHOLD) {
                startTime = time;
            }

            if (TimeUnit.NANOSECONDS.toSeconds(time - lastLogTime) > 1) {
                lastLogTime = time;
                logger.info("Word count: {}", data);
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
                return invocations / (elapsedNanos / 1.0e9);
            } else {
                return Double.NaN;
            }
        }

        @Override
        public boolean isConsuming() {
            return true;
        }
    }

    @SuppressWarnings("serial")
    static class WordCount extends HashMap<String, Integer> {
        public WordCount() {
        }

        public WordCount(WordCount runningWordCounts) {
            super(runningWordCounts);
        }
    }
}
