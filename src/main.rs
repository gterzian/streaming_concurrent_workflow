#[macro_use]
extern crate crossbeam_channel;
extern crate rayon;

use crossbeam_channel::{never, tick, unbounded, Receiver, Sender};
use std::collections::VecDeque;
use std::thread;
use std::time::Duration;

fn main() {}

/// Check the current size of the buffer, and modulate the source accordingly.
fn check_buffer_size(
    buffer: &mut VecDeque<u8>,
    sender: &Sender<RegulateSourceMsg>,
    tick_adjusted: &mut bool,
) {
    if !*tick_adjusted {
        return;
    }
    match buffer.len() {
        0 | 1 | 2 | 3 => {
            let _ = sender.send(RegulateSourceMsg::SpeedUp);
        }
        4 | 5 | 6 | 7 => {
            // Do nothing, we're happy with a buffer this size.
            return;
        }
        8 | 9 | 10 => {
            let _ = sender.send(RegulateSourceMsg::SlowDown);
        }
        _ => {
            let _ = sender.send(RegulateSourceMsg::Stop);
        }
    }
    *tick_adjusted = false;
}

/// Start work on the thread-pool.
fn start_work(
    result_sender: &Sender<ProcessorMsg>,
    pool_result_sender: &Sender<()>,
    ongoing_work: &mut usize,
    pool: &rayon::ThreadPool,
    num: u8,
) {
    // Clone the channels to move them into the worker.
    let result_sender = result_sender.clone();
    let pool_result_sender = pool_result_sender.clone();

    *ongoing_work += 1;

    pool.spawn(move || {
        // Perform some "work", sending the result to the "consumer".
        thread::sleep(Duration::from_millis(100));
        let _ = result_sender.send(ProcessorMsg::Result(num));
        let _ = pool_result_sender.send(());
    });
}

/// The messages sent from the "source",
/// to the "proccessor".
enum SourceMsg {
    /// Work to be processed.
    Work(u8),
    /// The source has defenitively stopped producing.
    Stopped,
    /// The tick was adjusted in response to a message.
    TickAdjusted,
}

/// The messages sent from the "processor",
/// to the "consumer"(effectively the main thread of the test).
enum ProcessorMsg {
    Result(u8),
    /// The processor has defenitively stopped processing.
    Stopped,
}

/// Messages from the processor to the source,
/// to modulate how fast the source produces.
#[derive(Debug)]
enum RegulateSourceMsg {
    SlowDown,
    SpeedUp,
    Stop,
}

/// Run the processor component.
fn run_processor(
    from_processor_sender: Sender<RegulateSourceMsg>,
    work_receiver: Receiver<SourceMsg>,
    result_sender: Sender<ProcessorMsg>,
) {
    // Spawn a "processor" component in parallel.
    let _ = thread::spawn(move || {
        // The processor has two worker threads at it's disposal.
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap();

        // Workers in the pool communicate that they've finished a unit of work,
        // back to the main-thread of the "processor", via this channel.
        let (pool_result_sender, pool_result_receiver) = unbounded();

        // A counter of ongoing work performed on the pool.
        let mut ongoing_work = 0;

        // A flag to keep track of whether the source has already stopped producing.
        let mut exiting = false;

        // A buffer for work while the workers are busy.
        let mut buffer = VecDeque::new();

        let mut tick_adjusted = true;

        loop {
            // Receive, and handle, messages,
            // until told to exit.
            select! {
                recv(work_receiver) -> msg => {
                    match msg {
                        Ok(SourceMsg::Work(num)) => {
                            if ongoing_work == 2 {
                                // All the workers are busy,
                                // buffer the work.
                                buffer.push_back(num);

                                // Potentially signal back-pressure.
                                check_buffer_size(&mut buffer, &from_processor_sender, &mut tick_adjusted);
                            } else {
                                start_work(&result_sender, &pool_result_sender, &mut ongoing_work, &pool, num);
                            }
                        },
                        Ok(SourceMsg::Stopped) => {
                            // Note that we've received the request to exit.
                            exiting = true;

                            // If there is no ongoing work,
                            // we can immediately exit.
                            if ongoing_work == 0 && buffer.is_empty() {
                                let _ = result_sender.send(ProcessorMsg::Stopped);
                                break;
                            }
                        }
                        Ok(SourceMsg::TickAdjusted) => {
                            tick_adjusted = true;
                        }
                        _ => {
                            // Note: will not happen thanks to `_work_sender_clone`.
                            panic!("Error receiving a SourceMsg.");
                        },
                    }
                },
                recv(pool_result_receiver) -> _ => {
                    if ongoing_work == 0 {
                        panic!("Received an unexpected pool result.");
                    }

                    // Note that a unit of work has been completed.
                    ongoing_work -=1;

                    if let Some(work) = buffer.pop_front() {
                        start_work(&result_sender, &pool_result_sender, &mut ongoing_work, &pool, work);
                    }

                    // Potentially signal back-pressure.
                    check_buffer_size(&mut buffer, &from_processor_sender, &mut tick_adjusted);

                    // If there is no more ongoing work,
                    // and we've received the request to exit,
                    // now is the time to exit.
                    if ongoing_work == 0 && exiting && buffer.is_empty() {
                        let _ = result_sender.send(ProcessorMsg::Stopped);
                        break;
                    }
                },
            }
        }
    });
}

/// Run the source component.
fn run_source(
    from_processor_receiver: Receiver<RegulateSourceMsg>,
    work_sender: Sender<SourceMsg>,
) {
    // Spawn a "source" component in parallel.
    let _ = thread::spawn(move || {
        // A counter of work produced.
        let mut counter: u8 = 0;

        let mut current_ticker_duration = Some(20);
        let mut ticker = tick(Duration::from_millis(current_ticker_duration.unwrap()));
        loop {
            select! {
                recv(ticker) -> _ => {
                    match counter.checked_add(1) {
                        Some(new_counter) => {
                            // Produce, and send for processing, a piece of "work".
                            let _ = work_sender.send(SourceMsg::Work(counter));
                            counter = new_counter
                        }
                        None => {
                            // Stop producing once we overflow.
                            let _ = work_sender.send(SourceMsg::Stopped);
                            break;
                        }
                    }
                },
                recv(from_processor_receiver) -> msg => {
                    let _ = work_sender.send(SourceMsg::TickAdjusted);
                    match msg {
                        Ok(RegulateSourceMsg::SlowDown) => {
                            current_ticker_duration = match current_ticker_duration {
                                Some(tick) => {
                                    if tick > 100 {
                                        Some(100)
                                    } else {
                                        Some(tick * 2)
                                    }
                                },
                                None => continue,
                            };
                            ticker = tick(Duration::from_millis(current_ticker_duration.unwrap()));
                        },
                        Ok(RegulateSourceMsg::SpeedUp) => {
                            current_ticker_duration = match current_ticker_duration {
                                Some(tick) if tick > 2 => Some(tick / 2),
                                Some(tick) => Some(tick),
                                // If we're in "stopped" mode, re-start slowly.
                                None => Some(100),
                            };
                            ticker = tick(Duration::from_millis(current_ticker_duration.unwrap()));
                        },
                        Ok(RegulateSourceMsg::Stop) => {
                            current_ticker_duration = None;
                            ticker = never();
                        },
                        _ => panic!("Error receiving a RegulateSourceMsg."),
                    }
                }
            }
        }
    });
}

#[test]
fn second() {
    let (work_sender, work_receiver) = unbounded();
    let (from_processor_sender, from_processor_receiver) = unbounded();
    let (result_sender, result_receiver) = unbounded();

    // Keeping a clone alive just to prevent a receive error after the source stops.
    let _work_sender_clone = work_sender.clone();

    // Start the processor.
    run_processor(from_processor_sender, work_receiver, result_sender);

    // Start the source.
    run_source(from_processor_receiver, work_sender);

    // The main test thread, doubling as the "consumer" component.

    // A counter of work received.
    let mut counter = 0;

    loop {
        match result_receiver.recv() {
            Ok(ProcessorMsg::Result(_num)) => {
                counter += 1;
            }
            Ok(ProcessorMsg::Stopped) => {
                // Processor has stopped.
                assert_eq!(counter, u8::MAX);
                break;
            }
            _ => panic!("Error receiving a ProcessorMsg."),
        }
    }
}
