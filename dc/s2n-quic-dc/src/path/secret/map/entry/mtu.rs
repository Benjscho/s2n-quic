// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use super::{Entry, SizeOf};
use parking_lot::Mutex;
use slotmap::{DefaultKey, SlotMap};
use std::sync::{atomic::Ordering, Arc, Weak};

pub(crate) trait Subscriber: Send + Sync {
    fn on_mtu_updated(&self, mtu: u16);
}

type List = SlotMap<DefaultKey, Weak<dyn Subscriber>>;

/// Allocate the registry only while streams are subscribed.
#[derive(Debug, Default)]
pub(super) struct Subscribers {
    inner: Mutex<Option<Box<List>>>,
}

impl SizeOf for Subscribers {
    fn size(&self) -> usize {
        let inner = self.inner.lock();
        let heap = inner.as_ref().map_or(0, |list| {
            // Approximate each slot as a weak pointer plus its version and alignment.
            std::mem::size_of::<List>()
                + (list.capacity() + 1) * std::mem::size_of::<(Weak<dyn Subscriber>, u64)>()
        });
        std::mem::size_of::<Self>() + heap
    }
}

/// Unregister when the send worker exits. Neither side keeps the other alive.
#[must_use]
pub(crate) struct Subscription {
    entry: Weak<Entry>,
    key: DefaultKey,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if let Some(entry) = self.entry.upgrade() {
            let mut inner = entry.mtu_subscribers.inner.lock();
            if let Some(list) = inner.as_mut() {
                list.remove(self.key);
                if list.is_empty() {
                    *inner = None;
                }
            }
        }
    }
}

impl Entry {
    pub(crate) fn subscribe_mtu(
        self: &Arc<Self>,
        subscriber: Weak<dyn Subscriber>,
        initial_mtu: u16,
    ) -> Subscription {
        let (key, mtu) = {
            let mut inner = self.mtu_subscribers.inner.lock();
            let list = inner.get_or_insert_with(|| Box::new(List::with_key()));
            let key = list.insert(subscriber.clone());
            // Read and register under the same lock used by updates, so a probe completing
            // between stream creation and registration cannot be missed.
            (key, self.parameters.max_datagram_size())
        };
        let subscription = Subscription {
            entry: Arc::downgrade(self),
            key,
        };

        // Call outside the registry lock: notifying can wake another thread immediately.
        // A newer notification can arrive first; receivers only adopt increases.
        if mtu > initial_mtu {
            if let Some(subscriber) = subscriber.upgrade() {
                subscriber.on_mtu_updated(mtu);
            }
        }
        subscription
    }

    pub fn update_max_datagram_size(&self, mtu: u16) {
        let subscribers: Vec<_> = {
            let inner = self.mtu_subscribers.inner.lock();
            let previous = self
                .parameters
                .max_datagram_size
                .swap(mtu, Ordering::Relaxed);
            if mtu <= previous {
                return;
            }
            inner
                .iter()
                .flat_map(|list| list.values())
                .filter_map(Weak::upgrade)
                .collect()
        };
        for subscriber in subscribers {
            subscriber.on_mtu_updated(mtu);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    struct Recorder {
        entry: Weak<Entry>,
        updates: Mutex<Vec<u16>>,
    }

    impl Subscriber for Recorder {
        fn on_mtu_updated(&self, mtu: u16) {
            // Subscriber callbacks must not run with the registry locked.
            assert!(self
                .entry
                .upgrade()
                .unwrap()
                .mtu_subscribers
                .inner
                .try_lock()
                .is_some());
            self.updates.lock().push(mtu);
        }
    }

    fn setup() -> (Arc<Entry>, Arc<Recorder>, Weak<dyn Subscriber>) {
        let entry = Entry::fake("127.0.0.1:443".parse().unwrap(), None);
        let recorder = Arc::new(Recorder {
            entry: Arc::downgrade(&entry),
            updates: Default::default(),
        });
        let subscriber: Arc<dyn Subscriber> = recorder.clone();
        (entry, recorder, Arc::downgrade(&subscriber))
    }

    #[test]
    fn catches_up_then_notifies_only_increases() {
        let (entry, recorder, subscriber) = setup();
        let initial = entry.parameters().max_datagram_size();
        entry.update_max_datagram_size(8000);
        let subscription = entry.subscribe_mtu(subscriber, initial);
        entry.update_max_datagram_size(8000);
        entry.update_max_datagram_size(8800);
        assert_eq!(*recorder.updates.lock(), [8000, 8800]);

        drop(subscription);
        entry.update_max_datagram_size(8900);
        assert_eq!(*recorder.updates.lock(), [8000, 8800]);
        assert!(entry.mtu_subscribers.inner.lock().is_none());
    }

    #[test]
    fn unchanged_mtu_does_not_notify() {
        let (entry, recorder, subscriber) = setup();
        let initial = entry.parameters().max_datagram_size();
        let _subscription = entry.subscribe_mtu(subscriber, initial);
        entry.update_max_datagram_size(initial);
        entry.update_max_datagram_size(initial - 1);
        assert!(recorder.updates.lock().is_empty());
    }

    #[test]
    fn subscription_does_not_retain_stream_or_entry() {
        let (entry, recorder, subscriber) = setup();
        let subscription =
            entry.subscribe_mtu(subscriber.clone(), entry.parameters().max_datagram_size());
        drop(recorder);
        assert!(subscriber.upgrade().is_none());
        entry.update_max_datagram_size(8800);
        let weak_entry = Arc::downgrade(&entry);
        drop(entry);
        assert!(weak_entry.upgrade().is_none());
        drop(subscription);
    }

    #[test]
    fn unregister_only_removes_its_own_subscription() {
        let (entry, recorder, subscriber) = setup();
        let initial = entry.parameters().max_datagram_size();
        let first = entry.subscribe_mtu(subscriber.clone(), initial);
        let second = entry.subscribe_mtu(subscriber, initial);
        drop(first);
        entry.update_max_datagram_size(8800);
        assert_eq!(*recorder.updates.lock(), [8800]);
        drop(second);
        assert!(entry.mtu_subscribers.inner.lock().is_none());
    }

    #[test]
    fn registration_racing_with_update_does_not_miss_mtu() {
        for _ in 0..100 {
            let (entry, recorder, subscriber) = setup();
            let initial = entry.parameters().max_datagram_size();
            let barrier = Arc::new(Barrier::new(2));
            let register = {
                let entry = entry.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    entry.subscribe_mtu(subscriber, initial)
                })
            };
            barrier.wait();
            entry.update_max_datagram_size(8800);
            let _subscription = register.join().unwrap();
            assert_eq!(recorder.updates.lock().last(), Some(&8800));
        }
    }
}
