package com.ioworks.noqueue;

import java.util.Iterator;

/**
 * Similar to {@link java.lang.ThreadLocal} except instead of {@link java.lang.ThreadLocal#set(Object)} add is used before all PipeLocal is used to set
 * individual values to be used by individual threads.
 * Created by jwang on 3/30/15.
 */
public interface PipeLocal<T> {

    /**
     * @return the value added or null if none is availabile
     */
    T get();

    /**
     * @param value The value used to get.  At most {@link #getSize()}.
     * @return true if the value was able to be added
     */
    boolean add(T value);

    /**
     * @return the number of individual instances
     */
    int getSize();

    /**
     * @return iterator through all the elements
     */
    Iterator<T> getIterator();

    /**
     * Include another PipeLocal as part of this pipe local
     * @param value the other PipeLocal to include
     */
    void include(PipeLocal value);
}
