#[macro_use]
extern crate crossbeam_channel;
extern crate rayon;

use crossbeam_channel::{tick, unbounded};
use std::collections::{HashMap, VecDeque};
use std::thread;
use std::time::Duration;

fn main() {}

#[test]
fn first() {
    /// The messages sent from the "source",
    /// to the "proccessor".
    enum SourceMsg {
        /// Work to be processed.
        Work(u8),
        /// The source has defenitively stopped producing.
        Stopped,
    }

    /// The messages sent from the "processor",
    /// to the "consumer"(effectively the main thread of the test).
    enum ProcessorMsg {
        Result(u8),
        /// The processor has defenitively stopped processing.
        Stopped,
    }

    let (work_sender, work_receiver) = unbounded();
    let (result_sender, result_receiver) = unbounded();

    // Keeping a clone alive just to prevent a receive error after the source stops.
    let _work_sender_clone = work_sender.clone();

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

        loop {
            // Receive, and handle, messages,
            // until told to exit.
            select! {
                recv(work_receiver) -> msg => {
                    match msg {
                        Ok(SourceMsg::Work(num)) => {
                            // Surprisingly, the length of the channel doesn't really go up.
                            // Where's the work piling-up?
                            println!("Queue: {:?}", work_receiver.len());

                            // Clone the channels to move them into the worker.
                            let result_sender = result_sender.clone();
                            let pool_result_sender = pool_result_sender.clone();

                            ongoing_work +=1;

                            pool.spawn(move || {
                                // Perform some "work", sending the result to the "consumer".
                                thread::sleep(Duration::from_millis(3));
                                let _ = result_sender.send(ProcessorMsg::Result(num));
                                let _ = pool_result_sender.send(());
                            });
                        },
                        Ok(SourceMsg::Stopped) => {
                            // Note that we've received the request to exit.
                            exiting = true;

                            // If there is no ongoing work,
                            // we can immediately exit.
                            if ongoing_work == 0 {
                                let _ = result_sender.send(ProcessorMsg::Stopped);
                                break;
                            }
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

                    // If there is no more ongoing work,
                    // and we've received the request to exit,
                    // now is the time to exit.
                    if ongoing_work == 0 && exiting {
                        let _ = result_sender.send(ProcessorMsg::Stopped);
                        break;
                    }
                },
            }
        }
    });

    // Spawn a "source" component in parallel.
    let _ = thread::spawn(move || {
        // A counter of work produced.
        let mut counter: u8 = 0;
        let ticker = tick(Duration::from_millis(1));
        loop {
            // Block on a tick.
            ticker.recv().unwrap();

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
        }
    });

    // The main test thread, doubling as the "consumer" component.

    // A counter of work received.
    let mut counter = 0;

    loop {
        match result_receiver.recv() {
            Ok(ProcessorMsg::Result(num)) => {
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
