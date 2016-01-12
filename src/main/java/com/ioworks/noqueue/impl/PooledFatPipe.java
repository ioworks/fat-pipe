package com.ioworks.noqueue.impl;

import java.beans.ExceptionListener;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ioworks.noqueue.*;

/**
 * Created by IntelliJ IDEA.
 * User: jwang
 * Date: 8/24/12
 * Time: 7:25 PM
 */
public class PooledFatPipe<T, S> implements FatPipe<T, S>, Runnable {
    private static final int ARBITARY_THREADS = 16;
    private static Signal dummySignal = new DummySignal();
    protected static Logger logger = LoggerFactory.getLogger("com.ioworks.fxworks.core.ServerComponent");
    private ThreadGroup group;
    private volatile AtomicReferenceArray<T> inputs, outputs;
    private CopyOnWriteArrayList<Receptor<T>> freeReceptors;
    @SuppressWarnings({"unchecked"})
    private Consumer<S>[] consumers = new Consumer[0];
    private long[] consumerSeqs = new long[0];
    private AtomicLongArray outUse = new AtomicLongArray(0);
    private Producer<T, S> producer;
    private ExceptionListener listener;
    private Semaphore lock;
    private Lock consumerLock = new ReentrantLock();
    private boolean started = false;
    private long sequence, finalSequence;
    private S finalProduct;
    private int poolSize = 0;
    private Pool pool;
    private long sleep = 0;
    private String name = "default";
    private SignalImpl[] signals;
    protected Thread[] threads;
    private List<ReceptorImpl> reuseReceptors;
    private ExecutorService executor;
    private boolean reduceContextSwitch;
    private boolean adding = false;
    private long startTime;
    private long sleptTime;
    private long toSleepTime;
    private int incrementTime;

    /**
     * Create a fat pipe using the threads from the parent
     * @param producer the producer
     * @param pool the group of pipes to add to
     */
    public PooledFatPipe(Producer<T, S> producer, Pool pool, ExceptionListener listener) {
        this.producer = producer;
        this.listener = listener;
        this.pool = pool;
        name = producer.getClass().getSimpleName();
        lock = new Semaphore(0);
    }

    /**
     * Create fat pipe using several threads
     * @param producer the producer
     * @param poolSize the size of the thread group.
     */
    public PooledFatPipe(Producer<T, S> producer, int poolSize, ExceptionListener listener) {
        this(producer, poolSize, listener, producer.getClass().getSimpleName());
    }

    public PooledFatPipe(Producer<T, S> producer, int poolSize, ExceptionListener listener, String name) {
        this.producer = producer;
        this.listener = listener;
        this.poolSize = poolSize;
        this.name = name;
        lock = new Semaphore(0);
    }

    @Override
    public String toString() {
        return name+" producer="+producer;
    }

    public void start() {
        inputs = new AtomicReferenceArray<>(0);
        outputs = inputs;
        freeReceptors = new CopyOnWriteArrayList<>();
        sequence = 0;
        finalSequence = 0;
        finalProduct = null;
        reuseReceptors = new ArrayList<>(0);
        executor = Executors.newSingleThreadExecutor();

        started = true;
        if(poolSize>0) {
            group = new ThreadGroup("FatPipe");
            group.setMaxPriority(Thread.MAX_PRIORITY);
        }
        signals = new SignalImpl[ARBITARY_THREADS+poolSize];
        threads = new Thread[ARBITARY_THREADS+poolSize];
        startTime = System.currentTimeMillis();
        sleptTime = 0;
        toSleepTime = 0;

        for(int i=0; i<poolSize; i++) {
            Thread t = new Thread(group, this, name+"-"+i);
            t.setDaemon(true);
            threads[i] = t;
            t.start();
            if(sleep==0) t.setPriority(Thread.MAX_PRIORITY);
            else if(sleep<0) t.setPriority(Thread.NORM_PRIORITY);
            else t.setPriority(Thread.MIN_PRIORITY);
        }
        for(int i=0; i<signals.length; i++) {
            signals[i] = new SignalImpl(lock);
        }
        if(pool!=null) pool.add(this);
        lock.release();
    }

    /**
     * for testing onldy
     */
    @Deprecated
    protected void setThreads(Thread[] threads) {
        this.threads = threads;
        signals = new SignalImpl[threads.length];
        for(int i=0; i<threads.length; i++) {
            signals[i] = new SignalImpl(lock);
        }
    }

    public Producer<T, S> getProducer() {
        return producer;
    }

    @Override
    public synchronized Receptor<T> takeReceptor() {
        if(!freeReceptors.isEmpty()) {
            ReceptorImpl impl = (ReceptorImpl) freeReceptors.remove(0);
            impl.skips = 0;
            return impl;
        }
        int length = inputs.length()+1;
        final AtomicReferenceArray<T> inputs = new AtomicReferenceArray<>(length);
        this.inputs = inputs;
        final List<ReceptorImpl> newReuseReceptors = new ArrayList<>(length);
        for(int i=0; i<length; i++) {
            newReuseReceptors.add(new ReceptorImpl(i));
        }
        executor.execute(() -> {
            long lastTime = System.currentTimeMillis();
            try {
                boolean empty = false;
                while (!empty && started) {
                    empty = true;
                    for(int i=0; i<outputs.length(); i++) {
                        T result = outputs.get(i);
                        if(result==null) continue;
                        Thread.sleep(1);
                        empty=false;
                        if(lastTime+30_000<System.currentTimeMillis()) {
                            lastTime = System.currentTimeMillis();
                            logger.error("unable to set receptors for "+producer+" i="+i+" result="+result);
                            if(pool!=null) logger.warn("pool="+pool.producer);
                        }
                        break;
                    }
                }
            } catch (InterruptedException e) {
                logger.warn("takeReceptor", e);
            } finally {
                reuseReceptors = newReuseReceptors;
                outputs = inputs;
            }
        });
        return new ReceptorImpl(length-1);
    }

    @Override
    public void setSleep(long sleep) {
        this.sleep = sleep;
    }

    private synchronized void dispose(Receptor<T> r) {
        if (!freeReceptors.addIfAbsent(r)) {
            try { throw new IllegalStateException(); } catch (IllegalStateException ex) { logger.warn("Attempt to add duplicate receptor: " + r, ex); }
        }
    }

    @Override
    public void run() {
        try {
            while (started) {
                int result = execute();
                if(result<0) break;
                if(result>0) {
                    if(sleep>0) {
                        Thread.sleep(sleep);
                        sleptTime += sleep;
                    } else if(sleep<0) Thread.yield();
                } else if(adding) {
                    Thread.sleep(1);
                } else {
                    if(sleep>0) {
                        incrementTime++;
                        if(incrementTime%1_000_000==0) {
                            toSleepTime += sleep;
                            if(sleptTime<toSleepTime) {
                                sleptTime+=sleep;
                                Thread.sleep(sleep);
                            }
                        }
                    }
                }
            }
        } catch (InterruptedException e) {
            if(started) {
                if(listener==null) logger.warn("polling", e);
                else listener.exceptionThrown(e);
            }
        } catch (Exception e) {
            if(listener==null) logger.error("polling", e);
            else listener.exceptionThrown(e);
        }
    }

    /**
     *
     * @return if <0, then this pipe should be stopped.  If ==0, it should not wait.  If >0, if it should wait
     */
    int execute() {
        if(!lock.tryAcquire()) return 1;       //currently being accessed
        Thread myThread = Thread.currentThread();
        int threadIndex = -1;
        for(int i=0; i<threads.length; i++) {
            if(threads[i]==null) {
                threads[i]=myThread;
                threadIndex = i;
                break;
            }
            if(myThread!=threads[i]) continue;
            threadIndex = i;
            break;
        }
        Signal signal;
        if(threadIndex<0) {
            signal = dummySignal;
        } else {
            SignalImpl s = signals[threadIndex];
            s.threadIndex = threadIndex;
            s.signaled = false;
            signal = s;
        }
        boolean hasData;
        try {
            hasData = poll(signal, null);
        } finally {
            signal.signal();
            if(threadIndex<0) lock.release();
        }
        return 0;
    }

    @Override
    public boolean setConsumers(List<Consumer<S>> list, long time, TimeUnit unit) throws IllegalArgumentException {
        if(list==null) throw new IllegalArgumentException("consumer list is not set");
        for(Consumer<S> consumer : list) {
            if(consumer==null) throw new IllegalArgumentException("consumer is null");
        }
        try {
            if(!lock.tryAcquire(time, unit)) throw new IllegalArgumentException("can't lock producer");
        } catch (InterruptedException e) {
            return false;
        }
        try {
            return setConsumers(list);
        } catch (InterruptedException e) {
            logger.warn("setting consumer", e);
        } finally {
            lock.release();
        }
        return false;
    }

    private boolean setConsumers(List<Consumer<S>> list) throws InterruptedException {
        AtomicLongArray oldOutUse = outUse;
        long[] oldConsumerSeqs = consumerSeqs;
        long[] useArray = new long[list.size()];
        for(int i=0; i<useArray.length; i++) useArray[i] = -1;
        long[] seq = new long[list.size()];
        for(int i=0; i<seq.length; i++) seq[i] = Long.MAX_VALUE;
        Consumer<S>[] oldConsumers = consumers;
        consumers = list.toArray(new Consumer[0]);
        outUse = new AtomicLongArray(useArray);
        consumerSeqs = seq;
        for(int i=0; i<consumers.length; i++) {
            boolean found = false;
            for(int j=0; j< oldConsumers.length; j++) {
                if(consumers[i]!=oldConsumers[j]) continue;
                found = true;
                long entered = oldOutUse.get(j);
                setSequence(i, entered, oldConsumerSeqs[j]);
                break;
            }
            if(!found) setSequence(i, 0, 0);    //not in use
        }
        return true;
    }

    void setSequence(int index, long entered, long sequence) {
        outUse.set(index, entered);
        consumerSeqs[index] = sequence;
    }

    long[] getSequence(int index) {
        long entered = outUse.get(index);
        long sequence = consumerSeqs[index];
        return new long[] {entered, sequence};
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public void shutdown(long time, TimeUnit unit) {
        started = false;
        try {
            lock.tryAcquire(time, unit);
        } catch (InterruptedException e) {
            logger.warn("shutdown", e);
        }
        if(group !=null) group.interrupt();
        consumers = new Consumer[0];
        consumerSeqs = new long[0];
        outUse = new AtomicLongArray(0);
        executor.shutdown();
    }

    /**
     * Look for data from all the inputs
     * @return true if data was consumed
     */
    private boolean poll(Signal signal, Signal parent) {
        AtomicReferenceArray<T> o = outputs;
        for(int i=0; i<o.length(); i++) {
            T input = o.getAndSet(i, null);
            if(input==null) continue;
            long seq = ++sequence;
            if(parent!= null) parent.signal();
            S product = producer.execute(input, reuseReceptors.get(i), signal);
            signal.signal();
            if (!consume(product, seq, 0)) {
                Thread.yield();
                if(!consume(product, seq, 0)) logger.info("failed to consume product (" + product + ") from producer (" + producer + ")");
            }
            producer.complete(product);
            return product!=null;
        }
        return false;
    }

    /**
     *
     * @param product
     * @param sequence
     * @return false if there is an error
     */
    boolean consume(S product, long sequence, long sleep) {
        if(product==null) return true;

        //make copies
        Consumer<S>[] consumers = this.consumers;
        AtomicLongArray outUse = this.outUse;
        long[] consumerSeqs = this.consumerSeqs;

        if(outUse.length()!=consumers.length) return false;
        for(int j=0; j<consumers.length; j++) {
            if(!consumers[j].isConsuming()) continue;
            long time = System.nanoTime();
            if(!outUse.compareAndSet(j, 0, time)) continue;
            try {
                if(sequence<=consumerSeqs[j]) {
                    outUse.lazySet(j, 0);
                    if(outUse!=this.outUse) resetConsumer(consumers[j]);
                    break;
                }
                consumerSeqs[j] = sequence;
                consumers[j].consume(product, time);
                if(sleep>0) Thread.sleep(sleep);
                outUse.lazySet(j, 0);
                if(outUse!=this.outUse) {
                    resetConsumer(consumers[j]);
                    break;
                }
            } catch (Exception e) {
                if(listener==null) logger.error("consume", e);
                else listener.exceptionThrown(e);
            }
        }
        finishConsuming(product, sequence);
        return true;
    }

    private void resetConsumer(Consumer<S> consumer) {
        Consumer<S>[] consumers = this.consumers;
        AtomicLongArray outUse = this.outUse;
        for(int i=0; i<consumers.length; i++) {
            if(consumer!=consumers[i]) continue;
            outUse.lazySet(i, 0);
            return;
        }
    }

    private void finishConsuming(S product, long sequence) {
        long runSequence = 0;
        S runProduct = null;
        consumerLock.lock();
        try {
            if(sequence>=finalSequence) {
                finalSequence = sequence;
                finalProduct = product;
            } else {
                runSequence = finalSequence;
                runProduct = finalProduct;

            }
        } finally {
            consumerLock.unlock();
        }
        consume(runProduct, runSequence, 0);   //run again with the same thread if needed
    }

    Consumer<S> check(long maxTime, PooledFatPipe parent) {
        if(parent!=null && parent.group==null) return null;
        AtomicLongArray outUse = this.outUse;
        Consumer<S>[] consumers = this.consumers;
        long now = System.nanoTime();
        if(consumers.length>outUse.length()) {
            logger.warn("skipping check " + outUse.length() + "/" + consumers.length);
            return null;
        }
        try {
            for(int i=0; i<consumers.length; i++) {
                long time = outUse.get(i);
                if(time==0) continue;
                if(time==-1) continue;
                if(now<time+maxTime*1_000_000) continue;
                if(parent!=null) parent.group.list();
                return consumers[i];
            }
        } catch (Exception e) {
            logger.error("check", e);
        }
        return null;
    }

    public void setReduceContextSwitch(boolean reduceContextSwitch) {
        this.reduceContextSwitch = reduceContextSwitch;
    }

    class ReceptorImpl implements Receptor<T> {

        private int index;
        private int skips = 0;

        ReceptorImpl(int index) {
            this.index = index;
        }

        @Override
        public boolean set(T data) {
            T previous = inputs.getAndSet(index, data);
            if(previous!=null) skips++;
            executeInSet();
            return previous!=null;
        }

        @Override
        public boolean setIfEmpty(T data) {
            boolean isSet = inputs.compareAndSet(index, null, data);
            if(isSet) executeInSet();
            return isSet;
        }

        private void executeInSet() {
            if(!reduceContextSwitch) return;
            if(pool==null) execute();
            else pool.parent.execute();
        }

        @Override
        public void lazySet(T data) {
            inputs.lazySet(index, data);
        }

        @Override
        public boolean setIfEmpty(T data, long time, TimeUnit units) {
            long end = System.currentTimeMillis()+TimeUnit.MILLISECONDS.convert(time, units);
            while (!setIfEmpty(data)) {
                if(end<System.currentTimeMillis()) return false;
            }
            return true;
        }

        @Override
        public int skipCount() {
            return skips;
        }

        @Override
        public void dispose() {
            PooledFatPipe.this.dispose(this);
        }

        public String toString() {
            return ReceptorImpl.class.getSimpleName()+" ("+index+"/"+(inputs.length()-1)+") skip:"+skipCount();
        }
    }

    static class ParentProducer implements Producer<Object , Boolean> {

        private PooledFatPipe[] children = new PooledFatPipe[0];
        private AtomicIntegerArray inUse = new AtomicIntegerArray(0);
        private int poolSize;
        private ExceptionListener listener;
        private ChildSignalImpl[] signals;
        private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        ParentProducer(int poolSize, ExceptionListener listener) {
            this.poolSize = poolSize+ARBITARY_THREADS;
            this.listener = listener;
            signals = new ChildSignalImpl[0];
        }

        @Override
        public String toString() {
            return "ParentProducer usage="+inUse;
        }

        @Override
        public Boolean execute(Object d, Receptor<Object> r, Signal parent) {
            r.lazySet(d);   //put the data back in for polling
            boolean data = false;
            for(int i=0; i<children.length; i++) {
                AtomicIntegerArray inUse = this.inUse;
                if(!inUse.compareAndSet(i, 0, 1)) {
                    if (inUse.get(i)==2) sleep();
                    continue;
                }
                PooledFatPipe child = children[i];
                if(child==null) {
                    inUse.set(i, 0);
                    continue;
                }
                Signal signal;
                if(parent.getThreadIndex()<0) {
                    signal = new ChildSignalImpl(inUse, i);
                } else {
                    ChildSignalImpl s = signals[i*poolSize+parent.getThreadIndex()];
                    if(s==null || s.inUse!=inUse) {
                        sleep();
                        inUse.set(i, 0);
                        continue;
                    }
                    s.index = i;
                    s.notified = false;
                    signal = s;
                }
                try {
                    data |= child.poll(signal, parent);
                } catch (Exception e) {
                    if(listener==null) logger.error("poll", e);
                    else listener.exceptionThrown(e);
                } finally {
                    signal.signal();
                }
            }
            return data ? Boolean.TRUE : null;
        }

        private void sleep() {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        @Override
        public void complete(Boolean product) {

        }

        private void add(PooledFatPipe child) {
            long start = System.currentTimeMillis();
            while(true) {
                int fails = 0;
                for(int i=0; i<inUse.length(); i++) {
                    if(!inUse.compareAndSet(i, 0, 2)) {
                        if(inUse.get(i)!=2) fails++;
                    }
                }
                if(fails==0) break;
                if(start+100<System.currentTimeMillis()) {
                    logger.warn("unable to add cleanly "+toString());
                    break;
                }
            }
            final AtomicIntegerArray updated = new AtomicIntegerArray(inUse.length()+1);
            final List<Integer> needsReset = new ArrayList<>();
            for(int i=0; i<inUse.length(); i++) {
                int value = inUse.get(i);
                if(value==1) {
                    updated.set(i, value);
                    needsReset.add(i);
                }
            }
            final AtomicIntegerArray oldUse = inUse;
            inUse = updated;
            signals = new ChildSignalImpl[signals.length+poolSize];
            for(int i=0; i<signals.length; i++) {
                signals[i] = new ChildSignalImpl(inUse, 0);
            }
            children = Arrays.copyOf(children, children.length+1);
            children[children.length-1] = child;
            scheduleReset(needsReset, oldUse, updated);
        }

        void scheduleReset(List<Integer> needsReset, AtomicIntegerArray oldUse, AtomicIntegerArray updated) {
            if(needsReset.size()==0) return;
            executor.schedule(()->{
                for (int i = 0; i<needsReset.size(); i++) {
                    int index = needsReset.get(i);
                    int value = oldUse.get(index);
                    if (value == 0) {
                        updated.set(index, 0);
                        needsReset.remove(i--);
                    }
                }
                scheduleReset(needsReset, oldUse, updated);
            }, 1, TimeUnit.MILLISECONDS);
        }

        private void shutdown() {
            for(PooledFatPipe child : children) {
                child.shutdown(0, TimeUnit.MILLISECONDS);
            }
            executor.shutdown();
        }
    }

    /**
     * signals when the particular child can be used again
     */
    static class ChildSignalImpl implements Signal {
        private boolean notified;
        private int index;
        private AtomicIntegerArray inUse;

        ChildSignalImpl(AtomicIntegerArray inUse, int index) {
            this.index = index;
            this.inUse = inUse;
        }
        @Override
        public void signal() {
            if(notified) return;
            notified = true;
            inUse.lazySet(index, 0);
        }

        @Override
        public int getThreadIndex() {
            return -1;
        }

    }

    static class DummySignal implements Signal {

        @Override
        public void signal() {
        }

        @Override
        public int getThreadIndex() {
            return -1;
        }
    }

    public static class Pool<U> {
        PooledFatPipe<Object, Boolean> parent;
        ParentProducer producer;

        /**
         * Create a thread group to share among multiple fat pipes
         * @param poolSize the number of threads to create. Less then zero will use all the available cores-1.  There will be at least 1 thread used
         * @param listener in case an exception is thrown while executing one of the Fat Pipes
         */
        public Pool(int poolSize, String name, ExceptionListener listener) {
            if(poolSize<0) poolSize = Runtime.getRuntime().availableProcessors()-1;
            poolSize = Math.max(poolSize, 1);
            producer = new ParentProducer(poolSize, listener);
            parent = new PooledFatPipe<>(producer, poolSize, null, name);
        }

        void setSleep(long sleep) {
            parent.setSleep(sleep);
        }

        Consumer<U> check(long maxTime) {
            for(PooledFatPipe child : producer.children) {
                Consumer<U> consumer = child.check(maxTime, parent);
                if(consumer!=null) return consumer;
            }
            return null;
        }

        public void start() {
            parent.start();
            Receptor<Object> parentRecptor = parent.takeReceptor();
            parentRecptor.set(new Object());
        }

        private void add(PooledFatPipe child) {
            parent.adding = true;
            try {
                parent.lock.acquire();
            } catch (InterruptedException e) {
                logger.warn("add", e);
                return;
            } finally {
                parent.adding = false;
            }
            try {
                producer.add(child);
            } finally {
                parent.lock.release();
            }
        }

        public void shutdown(long time, TimeUnit unit) {
            parent.shutdown(time, unit);
            producer.shutdown();
        }

    }
}
