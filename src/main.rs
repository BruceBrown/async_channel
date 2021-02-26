use smol::{channel, future, Executor};
use std::panic::catch_unwind;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

// Some constants used in testing main()

// The forwarder. It owns its channel's sender and receiver, an optional forwarder and notifier. It has an
// executor and counter for received messages. Notification is performed by closing the channel.
#[derive(Debug)]
struct Forwarder {
    pub id: usize,
    pub sender: channel::Sender<usize>,
    pub receiver: channel::Receiver<usize>,
    pub fwd: Option<Arc<Forwarder>>,
    pub notify: Option<channel::Sender<()>>,
    pub executor: Arc<Executor<'static>>,
    pub count: AtomicUsize,
}
impl Forwarder {
    // Create a bare-bones Forwarder.
    fn new(id: usize, queue_size: usize, executor: Arc<Executor<'static>>) -> Self {
        let (sender, receiver) = channel::bounded(queue_size);
        Self {
            id,
            sender,
            receiver,
            fwd: None,
            notify: None,
            executor,
            count: AtomicUsize::new(0),
        }
    }

    // Start a forwarder. This consumes the forwarder, returning it wrapped as an Arc<Forwarder>.
    fn start(self, message_count: usize) -> Arc<Forwarder> {
        let forwarder = Arc::new(self);
        let f = forwarder.clone();
        forwarder
            .executor
            .spawn(async move {
                loop {
                    match f.receiver.recv().await {
                        Err(channel::RecvError) => break,
                        Ok(val) => {
                            let count = f.count.fetch_add(1, Ordering::SeqCst) + 1;
                            // If we can forward it, do it, blocking if the channel is full
                            if let Some(ref fwd) = f.fwd {
                                match fwd.sender.try_send(val) {
                                    Ok(()) => {}
                                    Err(channel::TrySendError::Closed(_)) => {
                                        break;
                                    }
                                    Err(channel::TrySendError::Full(val)) => {
                                        fwd.sender.send(val).await.ok();
                                    }
                                }
                            } else {
                                if count == message_count {
                                    println!("notifying main");
                                    if let Some(ref sender) = f.notify {
                                        sender.close();
                                    }
                                }
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

// Create executors. Returning vecs of executors and threads, and a shutdown channel.
fn create_executors(
    executor_per_thread: bool,
    thread_count: usize,
) -> (
    Vec<Arc<Executor<'static>>>,
    Vec<JoinHandle<()>>,
    channel::Sender<()>,
) {
    let mut executors: Vec<Arc<Executor>> = Vec::new();
    let mut threads: Vec<std::thread::JoinHandle<()>> = Vec::new();
    let (stop, signal) = channel::unbounded::<()>();
    let executor = Arc::new(Executor::new());
    if !executor_per_thread {
        executors.push(executor.clone());
    }
    for id in 1..=thread_count {
        let signal = signal.clone();
        let executor = if executor_per_thread {
            let executor = Arc::new(Executor::new());
            executors.push(executor.clone());
            executor
        } else {
            executor.clone()
        };
        let handler = std::thread::Builder::new()
            .name(format!("thread-{}", id))
            .spawn(move || loop {
                catch_unwind(|| future::block_on(executor.run(async { signal.recv().await }))).ok();
            })
            .expect("cannot spawn executor thread");
        threads.push(handler);
    }
    (executors, threads, stop)
}

// Create forwarders, returning a vec of Arc<Forwarder> wrapped forwarders and a notification channel.
// Notification is via closing the channel, and is done when the last forwarder receieves the last message.
fn create_forwarders(
    count: usize,
    queue_size: usize,
    message_count: usize,
    executors: &Vec<Arc<Executor<'static>>>,
) -> (Vec<Arc<Forwarder>>, channel::Receiver<()>) {
    let mut forwarders: Vec<Arc<Forwarder>> = Vec::new();

    let mut f = Forwarder::new(1, queue_size, executors[0].clone());
    let (s, r) = channel::unbounded::<()>();
    f.notify = Some(s);
    let mut last = f.start(message_count);
    forwarders.push(last.clone());

    let executor_count = executors.len();
    for id in 2..=count {
        let mut f = Forwarder::new(id, queue_size, executors[id % executor_count].clone());
        f.fwd = Some(last);
        last = f.start(message_count);
        forwarders.push(last.clone());
    }
    println!(
        "created {} forwarders queue_size={}",
        forwarders.len(),
        queue_size
    );

    (forwarders, r)
}

// Send all of the messages to a forwarder. This is used to kick-start message passing.
fn send_messages(count: usize, forwarder: Arc<Forwarder>) {
    let f = forwarder.clone();
    forwarder
        .executor
        .spawn(async move {
            for val in 1..=count {
                if f.sender.send(val).await.is_err() {
                    println!("failed to send initial messages")
                }
            }
            println!("sent {} messages to first forwarder", count);
        })
        .detach();
}

// Wait for all of the messages to pass through all of the forwarders.
fn wait_for_completion(start: std::time::SystemTime, r: channel::Receiver<()>) {
    future::block_on(Executor::new().run(async { r.recv().await })).ok();
    println!("completed in {:#?}", start.elapsed().unwrap());
}

fn main() {
    const THREADS: usize = 4;
    const EXECUTOR_PER_THREAD: bool = true;
    const FORWARDERS: usize = 10_000;
    const MESSAGES: usize = 20_000;
    const CHANNEL_BOUNDS: usize = 10;

    let (executors, _threads, stop) = create_executors(EXECUTOR_PER_THREAD, THREADS);
    let (mut forwarders, done) =
        create_forwarders(FORWARDERS, CHANNEL_BOUNDS, MESSAGES, &executors);

    let first = forwarders.pop().unwrap();
    let start = std::time::SystemTime::now();
    send_messages(MESSAGES, first);
    wait_for_completion(start, done);
    stop.close();
}

#[test]
fn test_single_thread_small_queue() {
    let (executors, _threads, stop) = create_executors(false, 1);
    let (forwarder_count, queue_size, message_count) = (1000, 10, 1000);
    let (mut forwarders, done) =
        create_forwarders(forwarder_count, queue_size, message_count, &executors);

    let first = forwarders.pop().unwrap();
    let start = std::time::SystemTime::now();
    send_messages(message_count, first.clone());
    wait_for_completion(start, done);
    stop.close();
}

#[test]
fn test_single_thread_big_queue() {
    let (executors, _threads, stop) = create_executors(false, 1);
    let (forwarder_count, queue_size, message_count) = (1000, 100, 1000);
    let (mut forwarders, done) =
        create_forwarders(forwarder_count, queue_size, message_count, &executors);
    let first = forwarders.pop().unwrap();
    let start = std::time::SystemTime::now();
    send_messages(message_count, first);
    wait_for_completion(start, done);
    stop.close();
}

#[test]
fn test_multi_thread_single_executor() {
    let (executors, _threads, stop) = create_executors(false, 4);
    let (forwarder_count, queue_size, message_count) = (1000, 10, 1000);
    let (mut forwarders, done) =
        create_forwarders(forwarder_count, queue_size, message_count, &executors);

    let first = forwarders.pop().unwrap();
    let start = std::time::SystemTime::now();
    send_messages(message_count, first);
    wait_for_completion(start, done);
    stop.close();
}

#[test]
fn test_multi_thread_multi_executor() {
    let (executors, _threads, stop) = create_executors(true, 4);
    let (forwarder_count, queue_size, message_count) = (1000, 10, 1000);
    let (mut forwarders, done) =
        create_forwarders(forwarder_count, queue_size, message_count, &executors);

    let first = forwarders.pop().unwrap();
    let start = std::time::SystemTime::now();
    send_messages(message_count, first);
    wait_for_completion(start, done);
    stop.close();
}

#[test]
fn test_main() {
    main()
}
