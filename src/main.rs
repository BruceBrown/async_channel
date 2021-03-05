use smol::{channel, future, Executor};

use std::{
    fmt,
    panic::catch_unwind,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread::JoinHandle,
    time,
};

type FutureQueue = Vec<usize>;

trait ChannelSender: Send + Sync {
    fn send(&self, cmd: usize);
    fn drain(&self) -> FutureQueue;
}

impl fmt::Debug for dyn ChannelSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "#ChannelSender {{ .. }}") }
}

// An adapter that allows for async send via an unbounded channel which shouldn't overflow
struct SenderAdapter {
    fwd: Arc<channel::Sender<usize>>,
    queue: Mutex<FutureQueue>,
}

impl SenderAdapter {
    fn new(fwd: Arc<channel::Sender<usize>>) -> Self {
        let queue: Mutex<FutureQueue> = Mutex::new(Vec::with_capacity(10));
        Self { fwd, queue }
    }

    fn start(self) -> Arc<SenderAdapter> { Arc::new(self) }
}

impl ChannelSender for SenderAdapter {
    fn send(&self, cmd: usize) { self.queue.lock().unwrap().push(cmd); }
    fn drain(&self) -> FutureQueue { self.queue.lock().unwrap().drain(..).collect() }
}

// The forwarder. It owns its channel's sender and receiver, an optional forwarder and notifier. It has an
// executor and counter for received messages. Notification is performed by closing the channel.
struct Forwarder {
    pub id: usize,
    pub sender: Arc<channel::Sender<usize>>,
    pub receiver: channel::Receiver<usize>,
    pub fwd: Option<Arc<Forwarder>>,
    pub notify: Option<channel::Sender<()>>,
    pub message_count: usize,
    pub adapter: Option<Arc<dyn ChannelSender>>,
    pub executor: Arc<Executor<'static>>,
    pub count: AtomicUsize,
    pub spawn_send: bool,
}

impl Forwarder {
    // Create a bare-bones Forwarder.
    fn new(id: usize, queue_size: usize, executor: Arc<Executor<'static>>) -> Self {
        let (sender, receiver) = channel::bounded(queue_size);
        let sender = Arc::new(sender);
        let adapter = None;
        Self {
            id,
            sender,
            receiver,
            fwd: None,
            notify: None,
            message_count: 0,
            adapter,
            executor,
            count: AtomicUsize::new(0),
            spawn_send: false,
        }
    }

    fn receive(&self, val: usize) {
        let count = self.count.fetch_add(1, Ordering::SeqCst) + 1;

        // If we can forward it, do it, blocking if the channel is full
        if let Some(ref adapter) = self.adapter {
            adapter.send(val);
        } else if count == self.message_count {
            println!("notifying main");
            if let Some(ref sender) = self.notify {
                sender.close();
            }
        }
    }

    fn start(mut self) -> Arc<Forwarder> {
        let sender_adapter = if let Some(ref fwd) = self.fwd {
            let adapter = SenderAdapter::new(fwd.sender.clone()).start();
            self.adapter = Some(adapter.clone());
            Some(adapter)
        } else {
            None
        };
        let forwarder = Arc::new(self);
        let f = forwarder.clone();
        let r = forwarder.receiver.clone();
        forwarder
            .executor
            .spawn(async move {
                while let Ok(val) = r.recv().await {
                    f.receive(val);
                    if let Some(ref adapter) = sender_adapter {
                        for cmd in adapter.drain() {
                            if adapter.fwd.try_send(cmd).is_err() {
                                adapter.fwd.send(cmd).await.ok();
                            }
                        }
                    }
                }
                println!("fwd {} exited receive loop", f.id);
            })
            .detach();
        forwarder
    }
}

struct ForwarderFactory {
    pub forwarder_count: usize,
    pub queue_size: usize,
    pub message_count: usize,
    pub spawn_send: bool,
}
impl Default for ForwarderFactory {
    fn default() -> Self {
        Self {
            forwarder_count: 1000,
            queue_size: 10,
            message_count: 1000,
            spawn_send: false,
        }
    }
}

// ForwarderFactory modifiers.
#[allow(dead_code)]
impl ForwarderFactory {
    fn forwarder_count(mut self, forwarder_count: usize) -> Self {
        self.forwarder_count = forwarder_count;
        self
    }

    fn queue_size(mut self, queue_size: usize) -> Self {
        self.queue_size = queue_size;
        self
    }

    fn message_count(mut self, message_count: usize) -> Self {
        self.message_count = message_count;
        self
    }

    fn spawn_send(mut self, spawn_send: bool) -> Self {
        self.spawn_send = spawn_send;
        self
    }
}

impl ForwarderFactory {
    // Create forwarders, returning a vec of Arc<Forwarder> wrapped forwarders and a notification channel.
    // Notification is via closing the channel, and is done when the last forwarder receieves the last message.
    fn create_forwarders(&self, executors: &[Arc<Executor<'static>>]) -> (Vec<Arc<Forwarder>>, channel::Receiver<()>) {
        let mut forwarders: Vec<Arc<Forwarder>> = Vec::new();

        let mut f = Forwarder::new(1, self.queue_size, executors[0].clone());
        let (s, r) = channel::unbounded::<()>();
        f.notify = Some(s);
        f.message_count = self.message_count;
        f.spawn_send = self.spawn_send;
        let mut last = f.start();
        forwarders.push(last.clone());

        let executor_count = executors.len();
        for id in 2 ..= self.forwarder_count {
            let mut f = Forwarder::new(id, self.queue_size, executors[id % executor_count].clone());
            f.fwd = Some(last);
            f.spawn_send = self.spawn_send;
            last = f.start();
            forwarders.push(last.clone());
        }
        println!("created {} forwarders queue_size={}", forwarders.len(), self.queue_size);

        (forwarders, r)
    }
}

struct ExecutorFactory {
    thread_count: usize,
    bind_executor_to_thread: bool,
}

impl Default for ExecutorFactory {
    fn default() -> Self {
        Self {
            thread_count: 4,
            bind_executor_to_thread: true,
        }
    }
}

// ExecutorFactory modifiers.
#[allow(dead_code)]
impl ExecutorFactory {
    fn thread_count(mut self, thread_count: usize) -> Self {
        self.thread_count = thread_count;
        self
    }

    fn bind_executor_to_thread(mut self, bind_executor_to_thread: bool) -> Self {
        self.bind_executor_to_thread = bind_executor_to_thread;
        self
    }
}

impl ExecutorFactory {
    // Create executors. Returning vecs of executors and threads, and a shutdown channel.
    fn create_executors(&self, prefix: &str) -> (Vec<Arc<Executor<'static>>>, Vec<JoinHandle<()>>, channel::Sender<()>) {
        let mut executors: Vec<Arc<Executor>> = Vec::new();
        let mut threads: Vec<std::thread::JoinHandle<()>> = Vec::new();
        let (stop, signal) = channel::unbounded::<()>();
        let executor = Arc::new(Executor::new());
        if !self.bind_executor_to_thread {
            executors.push(executor.clone());
        }
        for id in 1 ..= self.thread_count {
            let signal = signal.clone();
            let executor = if self.bind_executor_to_thread {
                let executor = Arc::new(Executor::new());
                executors.push(executor.clone());
                executor
            } else {
                executor.clone()
            };
            let handler = std::thread::Builder::new()
                .name(format!("{}-{}", prefix, id))
                .spawn(move || loop {
                    catch_unwind(|| future::block_on(executor.run(async { signal.recv().await }))).ok();
                })
                .expect("cannot spawn executor thread");
            threads.push(handler);
        }
        (executors, threads, stop)
    }
}

// Send all of the messages to a forwarder. This is used to kick-start message passing.
// This is intended to be called from the main thread, and executor on the forwarder thread.
fn send_messages(count: usize, forwarder: Arc<Forwarder>) {
    let f = forwarder.clone();
    forwarder
        .executor
        .spawn(async move {
            let start = time::Instant::now();
            for val in 1 ..= count {
                if f.sender.send(val).await.is_err() {
                    println!("failed to send initial messages")
                }
            }
            let elapsed = start.elapsed();
            println!("sent {} messages to first forwarder elapsed={:#?}", count, elapsed);
        })
        .detach();
}

// Wait for all of the messages to pass through all of the forwarders. This is
// intended to be called and run on the main thread.
fn wait_for_completion(start: time::Instant, r: channel::Receiver<()>) {
    future::block_on(Executor::new().run(async { r.recv().await })).ok();
    println!("completed in {:#?}", start.elapsed());
}

fn main() {
    const FORWARDERS: usize = 10_000;
    const MESSAGES: usize = 20_000;
    const QUEUE_SIZE: usize = 1_000;

    let (executors, _threads, stop_send) = ExecutorFactory::default().create_executors("executor");
    let factory = ForwarderFactory::default()
        .forwarder_count(FORWARDERS)
        .message_count(MESSAGES)
        .queue_size(QUEUE_SIZE);
    let (mut forwarders, done) = factory.create_forwarders(&executors);

    let first = forwarders.pop().unwrap();
    let start = time::Instant::now();
    send_messages(MESSAGES, first);
    wait_for_completion(start, done);
    stop_send.close();
}

#[test]
fn test_single_thread_small_queue() {
    let (executors, _threads, stop_send) = ExecutorFactory::default().thread_count(1).create_executors("executor");
    let factory = ForwarderFactory::default();
    let (mut forwarders, done) = factory.create_forwarders(&executors);

    let first = forwarders.pop().unwrap();
    let start = time::Instant::now();
    send_messages(factory.message_count, first.clone());
    wait_for_completion(start, done);
    stop_send.close();
}

#[test]
fn test_single_thread_big_queue() {
    let (executors, _threads, stop_send) = ExecutorFactory::default().thread_count(1).create_executors("executor");
    let factory = ForwarderFactory::default().queue_size(100);
    let (mut forwarders, done) = factory.create_forwarders(&executors);

    let first = forwarders.pop().unwrap();
    let start = time::Instant::now();
    send_messages(factory.message_count, first);
    wait_for_completion(start, done);
    stop_send.close();
}

#[test]
fn test_single_thread_max_queue() {
    let (executors, _threads, stop_send) = ExecutorFactory::default().thread_count(1).create_executors("executor");
    let factory = ForwarderFactory::default().queue_size(1000);
    let (mut forwarders, done) = factory.create_forwarders(&executors);

    let first = forwarders.pop().unwrap();
    let start = time::Instant::now();
    send_messages(factory.message_count, first);
    wait_for_completion(start, done);
    stop_send.close();
}

#[test]
fn test_multi_thread_single_executor() {
    let (executors, _threads, stop_send) = ExecutorFactory::default()
        .bind_executor_to_thread(false)
        .create_executors("executor");

    let factory = ForwarderFactory::default();
    let (mut forwarders, done) = factory.create_forwarders(&executors);

    let first = forwarders.pop().unwrap();
    let start = time::Instant::now();
    send_messages(factory.message_count, first);
    wait_for_completion(start, done);
    stop_send.close();
}

#[test]
fn test_multi_thread_single_executor_max_queue() {
    let (executors, _threads, stop_send) = ExecutorFactory::default()
        .bind_executor_to_thread(false)
        .create_executors("executor");

    let factory = ForwarderFactory::default().queue_size(1000);
    let (mut forwarders, done) = factory.create_forwarders(&executors);

    let first = forwarders.pop().unwrap();
    let start = time::Instant::now();
    send_messages(factory.message_count, first);
    wait_for_completion(start, done);
    stop_send.close();
}

#[test]
fn test_multi_thread_multi_executor() {
    let (executors, _threads, stop_send) = ExecutorFactory::default().create_executors("executor");
    let factory = ForwarderFactory::default();
    let (mut forwarders, done) = factory.create_forwarders(&executors);

    let first = forwarders.pop().unwrap();
    let start = time::Instant::now();
    send_messages(factory.message_count, first);
    wait_for_completion(start, done);
    stop_send.close();
}

#[test]
fn test_multi_thread_multi_executor_max_queue() {
    let (executors, _threads, stop_send) = ExecutorFactory::default().create_executors("executor");
    let factory = ForwarderFactory::default().queue_size(1000);
    let (mut forwarders, done) = factory.create_forwarders(&executors);

    let first = forwarders.pop().unwrap();
    let start = time::Instant::now();
    send_messages(factory.message_count, first);
    wait_for_completion(start, done);
    stop_send.close();
}

#[test]
fn test_main_small_queue() {
    const FORWARDERS: usize = 10_000;
    const MESSAGES: usize = 20_000;
    const QUEUE_SIZE: usize = 100;

    let (executors, _threads, stop_send) = ExecutorFactory::default().create_executors("executor");
    let factory = ForwarderFactory::default()
        .forwarder_count(FORWARDERS)
        .message_count(MESSAGES)
        .queue_size(QUEUE_SIZE);
    let (mut forwarders, done) = factory.create_forwarders(&executors);

    let first = forwarders.pop().unwrap();
    let start = time::Instant::now();
    send_messages(MESSAGES, first);
    wait_for_completion(start, done);
    stop_send.close();
}

#[test]
fn test_main_max_queue() {
    const FORWARDERS: usize = 10_000;
    const MESSAGES: usize = 20_000;

    let (executors, _threads, stop_send) = ExecutorFactory::default().create_executors("executor");
    let factory = ForwarderFactory::default()
        .forwarder_count(FORWARDERS)
        .message_count(MESSAGES)
        .queue_size(MESSAGES);
    let (mut forwarders, done) = factory.create_forwarders(&executors);

    let first = forwarders.pop().unwrap();
    let start = time::Instant::now();
    send_messages(MESSAGES, first);
    wait_for_completion(start, done);
    stop_send.close();
}

#[test]
fn test_main() { main() }
