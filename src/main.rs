#[macro_use]
extern crate crossbeam_channel;
extern crate rayon;

use crossbeam_channel::unbounded;
use std::collections::HashMap;
use std::thread;

fn main() {}

#[test]
fn test_streaming_workflow() {
    enum WorkMsg {
        Work(u8),
        Exit,
    }

    enum ResultMsg {
        Result(u8),
        Exited,
    }

    let (work_sender, work_receiver) = unbounded();
    let (result_sender, result_receiver) = unbounded();
    // Add a new channel, used by workers
    // to notity the "parallel" component of having completed a unit of work.
    let (pool_result_sender, pool_result_receiver) = unbounded();
    let mut ongoing_work = 0;
    let mut exiting = false;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();

    let _ = thread::spawn(move || loop {
        select! {
            recv(work_receiver) -> msg => {
                match msg {
                    Ok(WorkMsg::Work(num)) => {
                        let result_sender = result_sender.clone();
                        let pool_result_sender = pool_result_sender.clone();

                        // Note that we're starting a new unit of work on the pool.
                        ongoing_work += 1;

                        pool.spawn(move || {
                            // 1. Send the result to the main component.
                            let _ = result_sender.send(ResultMsg::Result(num));

                            // 2. Let the parallel component know we've completed a unit of work.
                            let _ = pool_result_sender.send(());
                        });
                    },
                    Ok(WorkMsg::Exit) => {
                        // Note that we've received the request to exit.
                        exiting = true;

                        // If there is no ongoing work,
                        // we can immediately exit.
                        if ongoing_work == 0 {
                            let _ = result_sender.send(ResultMsg::Exited);
                            break;
                        }
                    },
                    _ => panic!("Error receiving a WorkMsg."),
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
                    let _ = result_sender.send(ResultMsg::Exited);
                    break;
                }
            },
        }
    });

    let _ = work_sender.send(WorkMsg::Work(0));
    let _ = work_sender.send(WorkMsg::Work(1));
    let _ = work_sender.send(WorkMsg::Exit);

    let mut counter = 0;

    loop {
        match result_receiver.recv() {
            Ok(ResultMsg::Result(_)) => {
                // Count the units of work that have been completed.
                counter += 1;
            }
            Ok(ResultMsg::Exited) => {
                // Assert that we're exiting after having received
                // all results.
                assert_eq!(2, counter);
                break;
            }
            _ => panic!("Error receiving a ResultMsg."),
        }
    }
}
