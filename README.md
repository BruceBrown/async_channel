# async_channel

Example of using SMOL channel and executor to implement async channels. A daisy_chain
of objects is used to test various flows. You can control:
* the number of nodes;
* the number of messages sent through the nodes;
* the queue size;
* the number of threads;
* the executor either:
  * shared by all threads;
  * executor per thread.

Unit tests test each of these configurations. One of which tests main(), which runs 
4 threads, with an executor in each, 10,000 nodes, sending 20,000 messages and channel
size of 10. Locally, on my macbook air it runs those 200 million messages in ~10 seconds.
