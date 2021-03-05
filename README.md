# async_channel

## The Motivation
This is the first of a series of experiments with asynchronous pipeline servers. This is a rather
unrealistic example, where there is a single pipeline, which can be very long. However,
it allows exploration of the impact of different executor stategies as well as the impact
of queue size upon overall throughput.

## Threads, Executors and Channels
Example of using SMOL channel and executor to implement async channels. A daisy_chain
of objects is used to test various flows. You can control:
* the number of nodes;
* the number of messages sent through the nodes;
* the queue size;
* the number of threads;
* the executor either:
  * shared by all threads;
  * executor per thread.

## Test Driven Experiments
Unit tests test each of these configurations. One of which tests main(), which runs 
4 threads, with an executor in each, 10,000 nodes, sending 20,000 messages and channel
size of 1000. Depending upon your hardware, different tests will run in different amounts of time.
There are some interesting results when the queue size is set to the maximum number of messages
that will be sent.

This utilizes the most basic of async machines, a single type of machine, with a single instruction set that is
hard wired.
