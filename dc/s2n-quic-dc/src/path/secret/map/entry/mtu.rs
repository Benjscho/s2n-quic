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

/// Only a live handshake with discovery enabled owns a registry. PSK paths configured
/// without discovery and paths restored from storage never allocate one; completed
/// handshakes release theirs.
///
/// Registration, the MTU snapshot, and updates share this lock. Callbacks run after
/// unlocking and may arrive out of order, so send workers only adopt increases.
/// Workers hold weak subscriptions, and removing a subscription need not wait for
/// callbacks already collected by an update. Such callbacks can only enqueue an event
/// on a still-live stream; they cannot retain a worker or create an ownership cycle.
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
            }
        }
    }
}

impl Entry {
    /// Called before publishing the entry to the map.
    pub(crate) fn start_mtu_discovery(&self) {
        let mut inner = self.mtu_subscribers.inner.lock();
        debug_assert!(inner.is_none());
        *inner = Some(Box::new(List::with_key()));
    }

    /// The handshake can no longer update this entry. Existing streams keep their last
    /// MTU; new streams can catch up without allocating or retaining a subscription.
    pub(crate) fn finish_mtu_discovery(&self) {
        *self.mtu_subscribers.inner.lock() = None;
    }

    #[cfg(test)]
    pub(crate) fn mtu_discovery_active(&self) -> bool {
        self.mtu_subscribers.inner.lock().is_some()
    }

    pub(crate) fn subscribe_mtu(
        self: &Arc<Self>,
        subscriber: Weak<dyn Subscriber>,
        initial_mtu: u16,
    ) -> Option<Subscription> {
        let (key, mtu) = {
            let mut inner = self.mtu_subscribers.inner.lock();
            let key = inner.as_mut().map(|list| list.insert(subscriber.clone()));
            // Read and register under the same lock used by updates, so a probe completing
            // between stream creation and registration cannot be missed.
            (key, self.parameters.max_datagram_size())
        };
        let subscription = key.map(|key| Subscription {
            entry: Arc::downgrade(self),
            key,
        });

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
        entry.start_mtu_discovery();
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
        assert!(entry
            .mtu_subscribers
            .inner
            .lock()
            .as_ref()
            .unwrap()
            .is_empty());
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
    fn completed_discovery_catches_up_without_subscribing() {
        let (entry, recorder, subscriber) = setup();
        let initial = entry.parameters().max_datagram_size();
        let subscription = entry.subscribe_mtu(subscriber.clone(), initial);
        entry.update_max_datagram_size(8800);
        entry.finish_mtu_discovery();
        assert!(!entry.mtu_discovery_active());
        drop(subscription);
        assert!(entry.subscribe_mtu(subscriber, initial).is_none());
        assert_eq!(*recorder.updates.lock(), [8800, 8800]);
        assert!(!entry.mtu_discovery_active());
    }

    #[test]
    fn default_entry_does_not_allocate_a_registry() {
        let entry = Entry::fake("127.0.0.1:443".parse().unwrap(), None);
        let recorder: Arc<dyn Subscriber> = Arc::new(Recorder {
            entry: Arc::downgrade(&entry),
            updates: Default::default(),
        });
        assert!(entry
            .subscribe_mtu(
                Arc::downgrade(&recorder),
                entry.parameters().max_datagram_size()
            )
            .is_none());
        assert!(!entry.mtu_discovery_active());
    }

    #[tokio::test]
    async fn cancelled_worker_unregisters_while_mtu_is_updated() {
        for _ in 0..100 {
            let (entry, recorder, subscriber) = setup();
            let subscription =
                entry.subscribe_mtu(subscriber.clone(), entry.parameters().max_datagram_size());
            let (ready, started) = tokio::sync::oneshot::channel();
            let worker = tokio::spawn(async move {
                let _subscription = subscription;
                let _subscriber = recorder;
                ready.send(()).unwrap();
                std::future::pending::<()>().await;
            });
            started.await.unwrap();
            let update = {
                let entry = entry.clone();
                std::thread::spawn(move || {
                    entry.update_max_datagram_size(8800);
                    entry.finish_mtu_discovery();
                })
            };
            worker.abort();
            assert!(worker.await.unwrap_err().is_cancelled());
            update.join().unwrap();
            assert!(subscriber.upgrade().is_none());
            assert!(!entry.mtu_discovery_active());
        }
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
        assert!(entry
            .mtu_subscribers
            .inner
            .lock()
            .as_ref()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn registration_racing_with_final_update_does_not_miss_mtu() {
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
            entry.finish_mtu_discovery();
            let _subscription = register.join().unwrap();
            assert_eq!(recorder.updates.lock().last(), Some(&8800));
            assert!(!entry.mtu_discovery_active());
        }
    }
}
