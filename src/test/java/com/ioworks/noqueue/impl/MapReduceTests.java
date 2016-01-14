package com.ioworks.noqueue.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.ioworks.noqueue.ProducerSets;
import org.junit.Test;

import com.ioworks.noqueue.FatPipe;
import com.ioworks.noqueue.FatPipe.Producer;
import com.ioworks.noqueue.ProducerSet;
import com.ioworks.noqueue.Receptor;

public class MapReduceTests {
    int threads = 4;
    int poolSize = 2;
    int iterations = 20_000;

    @Test
    public void performanceComparisonTest() throws InterruptedException, InstantiationException, IllegalAccessException {
        // initialize producer set
        ProducerSet<String, WordCount, WordCount> set = ProducerSets.newProducerSet(new FatMapReducerGenerator(), 1, 0, null);
        set.addConsumer(new ReducedConsumer(), null, 0);

        WordCount data1 = new WordCount();
        data1.put("Hello", 1);
        data1.put("World", 1);
        data1.put("Apache", 1);

        WordCount data2 = new WordCount();
        data2.put("The", 1);
        data2.put("Whole", 1);
        data2.put("World", 1);

        ExecutorService producers = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            int threadNo = i;
            producers.submit(() -> {
                Receptor<WordCount> receptor = set.get("reducer A");
                for (int r = 0; r < iterations; r++) {
                    receptor.lazySet(threadNo % 2 == 0 ? data1 : data2);
                    Thread.sleep(1);
                }

                return null;
            });
        }

        producers.shutdown();
        producers.awaitTermination(10, TimeUnit.SECONDS);
        TimeUnit.SECONDS.sleep(5);
    }

    static class FatMapReducerGenerator implements ProducerSet.Generator<String, WordCount, WordCount> {
        @Override
        public Producer<WordCount, WordCount> generate(String key) {
            return new FatMapReducer();
        }

        @Override
        public void exceptionThrown(Exception e) {
            e.printStackTrace();
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
            return new WordCount(runningWordCounts);
        }

        @Override
        public void complete(WordCount product) {
        }

    }

    static class ReducedConsumer implements FatPipe.Consumer<WordCount> {
        long lastPrintTime = 0;

        @Override
        public void consume(WordCount data, long time) {
            if (TimeUnit.NANOSECONDS.toSeconds(time - lastPrintTime) > 1) {
                System.out.println("Word count: " + data);
                lastPrintTime = time;
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
