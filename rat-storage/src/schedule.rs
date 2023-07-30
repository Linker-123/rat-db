use crossbeam::channel::{unbounded, SendError, Sender};
use lazy_static::lazy_static;
use parking_lot::RwLock;
use std::time::{Duration, Instant};
use tracing::info;

static mut SENDER: Option<Sender<IntervalMessage>> = None;
static mut MANAGER: Option<IntervalManager> = None;
lazy_static! {
    static ref ID_COUNTER: RwLock<usize> = RwLock::new(0);
}

struct ScheduledInterval {
    start: Instant,
    id: usize,
    // tick_time: TickTime,
    time: Duration,
    callback: Box<dyn Fn() + Send + 'static>,
}

pub enum IntervalMessage {
    Schedule(SlowInterval),
    End(usize),
}

pub struct IntervalManager {
    slow_intervals: Vec<ScheduledInterval>,
    sleep_time: Duration,
}

impl IntervalManager {
    pub fn init() {
        let (tx, rx) = unbounded::<IntervalMessage>();
        let manager = IntervalManager {
            slow_intervals: Vec::new(),
            sleep_time: Duration::from_millis(10),
        };

        rayon::spawn(move || loop {
            let manager = unsafe { MANAGER.as_mut().unwrap() };
            // check for new schedules
            let new_interval = rx.try_recv();

            if let Ok(intrv) = new_interval {
                match intrv {
                    IntervalMessage::Schedule(slow) => {
                        manager.slow_intervals.push(ScheduledInterval {
                            start: Instant::now(),
                            id: slow.id,
                            // tick_time: TickTime::Slow1s,
                            time: slow.time,
                            callback: slow.callback,
                        });

                        manager
                            .slow_intervals
                            .sort_by(|a, b| a.time.partial_cmp(&b.time).unwrap());
                        let interval = manager.slow_intervals.first().unwrap();
                        let min_sleep = interval.time;

                        if manager.sleep_time != min_sleep {
                            info!(
                                "Optimized sleep time from {:.2?} to {:.2?}",
                                manager.sleep_time, min_sleep
                            );
                            manager.sleep_time = min_sleep;
                        }
                    }
                    IntervalMessage::End(id) => {
                        manager.slow_intervals.retain(|intrv| intrv.id != id)
                    }
                }
            }

            // execute scheduled
            for interval in manager.slow_intervals.iter_mut() {
                if interval.start.elapsed() >= interval.time {
                    (*interval.callback)();
                    interval.start = Instant::now();
                }
            }

            std::thread::sleep(manager.sleep_time);
        });

        unsafe {
            MANAGER.replace(manager);
            SENDER.replace(tx);
        }
    }
}

// pub enum TickTime {
//     Slow1s,
// }

pub struct SlowInterval {
    time: Duration,
    id: usize,
    callback: Box<dyn Fn() + Send + 'static>,
}

impl SlowInterval {
    pub fn new<F>(time: Duration, callback: F) -> SlowInterval
    where
        F: Fn() + Send + 'static,
    {
        let mut id = ID_COUNTER.write();
        *id += 1;

        SlowInterval {
            time,
            id: *id,
            callback: Box::new(callback),
        }
    }

    pub fn start(self) -> Result<usize, SendError<IntervalMessage>> {
        let id = self.id;
        unsafe {
            SENDER
                .as_ref()
                .expect("Interval manager has not been initiated")
                .send(IntervalMessage::Schedule(self))?;
        }

        Ok(id)
    }

    pub fn end(id: usize) -> Result<(), SendError<IntervalMessage>> {
        unsafe {
            SENDER
                .as_ref()
                .expect("Interval manager has not been initiated")
                .send(IntervalMessage::End(id))?;
        }

        Ok(())
    }
}
