# FatPipe

<a href="https://github.com/ioworks/fat-pipe/releases">Downloads</a><br/>
<a href="http://ioworks.github.io/fat-pipe/api/">API</a>

High Performance Pooled-Thread Library

FatPipe is simple library for operating on a high volume of data in very latency sensitve applications.   For a low-latency application receiving, processing, and distributing data quickly is the highest priority.  In many cases, there will be a lot of data to work with, so it has to also be able to deal with a high volume of data. FatPipe increases the girth of the messaging pipe to allow more data and shorten the latency.

FatPipe supplies interfaces to receive data, handle processing of that data, and consumption of that data.  This allows the workflow to remain simple while still using multi-threaded processing.

# Remove the waiting
In many cases, the most important information is the most recent information.  In our case, one provider can send us multiple market data messages for one symbol in a single second (and there are multiple providers and hundreds fo symbols).  The most important information is the latest market data that shows us what the current price and the current volume.  What do we do with the other data?  If we don't have the time to process it, we discard it.  This allows us to remove the wait.  If there is no queue, there is no wait.

# Maximize resources
How much data that can be processed is subject to the limits of the slowest procedure.  The CPU core can process millions of instructions per second.  And you can have many cores in a single server.  If you have a procedure tied to a core/thread, then that core could be busy while everything else is idle.  For FatPipe, the core/thread is not tied to any procedure.  If there is something to work on, it will do the work.

# Streamline processing
In operating systems, context switching is very expensive.  One thread could process on one dataset and then pass the dataset to another thread.  To pass this data back and forth requires data to be loaded and stored.  FatPipe reduces this by having the thread follow the data instead.  The procedure is changed (which is can be much smaller) and the data being worked on remains the same.
