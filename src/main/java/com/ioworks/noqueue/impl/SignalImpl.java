package com.ioworks.noqueue.impl;

import java.util.concurrent.Semaphore;

import com.ioworks.noqueue.*;

/**
* Created by IntelliJ IDEA.
* User: jwang
* Date: 8/25/12
* Time: 8:47 PM
* Unlock only once
*/
class SignalImpl implements FatPipe.Signal {
    boolean signaled;
    private Semaphore lock;
    public int threadIndex;

    public SignalImpl(Semaphore lock) {
        this.lock = lock;
    }

    @Override
    public void signal() {
        if(signaled) return;
        signaled = true;
        lock.release();
    }

    @Override
    public int getThreadIndex() {
        return threadIndex;
    }

}
