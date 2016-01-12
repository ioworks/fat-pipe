package com.ioworks.noqueue.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.ioworks.noqueue.*;

/**
 * Created by jwang on 3/30/15.
 */
public class PipeLocalImpl<T> implements PipeLocal<T> {
    private Thread[] threads;
    private T[] objects;
    private List<PipeLocalImpl> includedPipes = new ArrayList<>();
    private List<T[]> includedObjects = new ArrayList<>();

    public PipeLocalImpl(Thread[] threads) {
        this.threads = threads;
        objects = (T[]) new Object[threads.length];
    }

    @Override
    public T get() {
        Thread current = Thread.currentThread();
        for(int i=0; i<threads.length; i++) {
            if(current==threads[i]) return objects[i];
        }
        int size = includedPipes.size();
        for(int i=0; i< size; i++) {
            PipeLocalImpl pipe = includedPipes.get(i);
            for(int j=0; j<pipe.threads.length; j++) {
                if(current==pipe.threads[j]) {
                    return includedObjects.get(i)[j];
                }
            }
        }
        return null;
    }

    @Override
    public boolean add(T value) {
        for(int i=0; i<objects.length; i++) {
            if(objects[i]!=null) continue;
            objects[i] = value;
            return true;
        }
        for(int i=0; i< includedObjects.size(); i++) {
            T[] array = includedObjects.get(i);
            for(int j=0; j<array.length; j++) {
                if(array[j]!=null) continue;
                array[j] = value;
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        toString(this, objects, builder);
        return builder.toString();
    }

    private void toString(PipeLocalImpl pipe, T[] objects, StringBuilder builder) {
        builder.append("[");
        for(int i=0; i<pipe.threads.length; i++) {
            if(pipe.threads[i]!=null) builder.append(pipe.threads[i].getName());
            builder.append("/");
            builder.append(objects[i]);
            builder.append(", ");
        }
        builder.append("(");
        for(int i=0; i<pipe.includedPipes.size(); i++) {
            PipeLocalImpl p = (PipeLocalImpl) pipe.includedPipes.get(i);
            T[] o = (T[]) pipe.includedObjects.get(i);
            toString(p, o, builder);
        }
        builder.append(")");
        builder.append("]");
    }

    @Override
    public int getSize() {
        int size = threads.length;
        for(int i=0; i<includedPipes.size(); i++) size += includedPipes.get(i).threads.length;
        return size;
    }

    @Override
    public Iterator<T> getIterator() {
        return Arrays.asList(objects).iterator();
    }

    @Override
    public void include(PipeLocal value) {
        PipeLocalImpl other = (PipeLocalImpl) value;
        includedPipes.add(other);
        includedObjects.add((T[]) new Object[other.threads.length]);
    }
}
