#![feature(const_fn, type_alias_impl_trait)]

use std::future::Future;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub trait Delay {
    type Future: Future;

    fn delay(&self) -> Self::Future;
}

impl<F, Fut> Delay for F
where
    F: Fn() -> Fut,
    Fut: Future<Output = ()>,
{
    type Future = impl Future<Output = ()>;

    fn delay(&self) -> Self::Future {
        self()
    }
}

pub struct Snowflake<D: Delay> {
    epoch: u128,
    node_id: u16,
    delay: D,
    counter: AtomicU16,
    last_timestep: AtomicU64,
}

impl<D: Delay> Snowflake<D> {
    pub const fn new(epoch: u128, node_id: u16, delay: D) -> Self {
        Self {
            epoch,
            node_id,
            delay,
            counter: AtomicU16::new(0),
            last_timestep: AtomicU64::new(0),
        }
    }

    pub async fn generate(&self) -> u64 {
        let (timestamp, sequence) = loop {
            let timestamp = {
                let unix_timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_else(|_| unreachable!())
                    .as_millis();
                (unix_timestamp - self.epoch) as u64
            };
            if timestamp == self.last_timestep.swap(timestamp, Ordering::SeqCst) {
                let sequence = (self.counter.load(Ordering::SeqCst) + 1) & 4095;
                self.counter.store(sequence, Ordering::SeqCst);
                if sequence != 0 {
                    break (timestamp, sequence);
                } else {
                    self.delay.delay().await
                }
            } else {
                self.counter.store(0, Ordering::SeqCst);
                break (timestamp, 0);
            };
        };
        const NODE_ID_WIDTH: u8 = 10;
        const SEQUENCE_WIDTH: u8 = 12;
        (timestamp << (NODE_ID_WIDTH + SEQUENCE_WIDTH))
            | ((self.node_id as u64) << SEQUENCE_WIDTH)
            | sequence as u64
    }
}
