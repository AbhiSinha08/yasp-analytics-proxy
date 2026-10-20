//! Occupied source capacity across all adapters. Permits follow resource lifetime.
use std::{
    collections::HashMap,
    io,
    sync::{Arc, Mutex},
};
use tokio::sync::{Notify, Semaphore};
#[cfg(feature = "snowflake")]
use tokio::time::{Instant, timeout_at};

/// One total for a configured backend registry. Share this handle to share capacity.
pub struct ConnectionBudget {
    limit: usize,
    state: Mutex<State>,
    reclaimers: Mutex<Vec<Reclaimer>>,
    pub(crate) changed: Notify,
}
struct State {
    used: usize,
    next_group: usize,
    groups: HashMap<usize, GroupState>,
}
struct GroupState {
    used: usize,
    limit: usize,
    closed: bool,
}
type Reclaim = Arc<dyn Fn() -> bool + Send + Sync>;
struct Reclaimer {
    group: usize,
    reclaim: Reclaim,
}

impl ConnectionBudget {
    /// Validate the total without reserving capacity or opening connections.
    pub fn new(limit: usize) -> io::Result<Arc<Self>> {
        if limit == 0 || limit > Semaphore::MAX_PERMITS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid backend connection limit",
            ));
        }
        Ok(Arc::new(Self {
            limit,
            state: Mutex::new(State {
                used: 0,
                next_group: 0,
                groups: HashMap::new(),
            }),
            reclaimers: Mutex::new(Vec::new()),
            changed: Notify::new(),
        }))
    }
    pub(crate) fn limit(&self) -> usize {
        self.limit
    }
    pub(crate) fn group(self: &Arc<Self>, limit: usize) -> io::Result<CapacityGroup> {
        if limit == 0 || limit > Semaphore::MAX_PERMITS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid backend group limit",
            ));
        }
        let mut state = self.state.lock().unwrap();
        let id = state.next_group;
        state.next_group += 1;
        state.groups.insert(
            id,
            GroupState {
                used: 0,
                limit,
                closed: false,
            },
        );
        Ok(CapacityGroup(Arc::new(Group {
            budget: self.clone(),
            id,
        })))
    }
    fn reclaim(&self, id: usize) {
        let local_full = {
            let state = self.state.lock().unwrap();
            let group = &state.groups[&id];
            if group.closed || (state.used < self.limit && group.used < group.limit) {
                return;
            }
            group.used >= group.limit
        };
        // Call adapters outside all accounting/registration locks: closing an
        // idle resource drops its permit and re-enters the budget.
        let callbacks: Vec<_> = self
            .reclaimers
            .lock()
            .unwrap()
            .iter()
            .filter(|r| !local_full || r.group == id)
            .map(|r| r.reclaim.clone())
            .collect();
        for reclaim in callbacks {
            if reclaim() {
                break;
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct CapacityGroup(Arc<Group>);
struct Group {
    budget: Arc<ConnectionBudget>,
    id: usize,
}
impl Drop for Group {
    fn drop(&mut self) {
        let group = self
            .budget
            .state
            .lock()
            .unwrap()
            .groups
            .remove(&self.id)
            .unwrap();
        debug_assert_eq!(group.used, 0);
        self.budget
            .reclaimers
            .lock()
            .unwrap()
            .retain(|r| r.group != self.id);
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum CapacityError {
    Full,
    Closed,
    #[cfg(feature = "snowflake")]
    TimedOut,
}

impl CapacityGroup {
    pub(crate) fn budget(&self) -> Arc<ConnectionBudget> {
        self.0.budget.clone()
    }
    pub(crate) fn register_reclaimer(&self, reclaim: impl Fn() -> bool + Send + Sync + 'static) {
        self.0.budget.reclaimers.lock().unwrap().push(Reclaimer {
            group: self.0.id,
            reclaim: Arc::new(reclaim),
        });
    }
    pub(crate) fn may_open(&self) -> bool {
        self.0.budget.reclaim(self.0.id);
        let state = self.0.budget.state.lock().unwrap();
        let group = &state.groups[&self.0.id];
        !group.closed && state.used < self.0.budget.limit && group.used < group.limit
    }
    pub(crate) fn try_acquire(&self) -> Result<CapacityPermit, CapacityError> {
        self.0.budget.reclaim(self.0.id);
        let mut state = self.0.budget.state.lock().unwrap();
        let group = &state.groups[&self.0.id];
        if group.closed {
            return Err(CapacityError::Closed);
        }
        if state.used >= self.0.budget.limit || group.used >= group.limit {
            return Err(CapacityError::Full);
        }
        state.used += 1;
        state.groups.get_mut(&self.0.id).unwrap().used += 1;
        Ok(CapacityPermit {
            group: self.clone(),
        })
    }
    #[cfg(feature = "snowflake")]
    pub(crate) async fn acquire(&self, deadline: Instant) -> Result<CapacityPermit, CapacityError> {
        loop {
            let notified = self.0.budget.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.try_acquire() {
                Err(CapacityError::Full) => {}
                result => return result,
            }
            timeout_at(deadline, notified)
                .await
                .map_err(|_| CapacityError::TimedOut)?;
        }
    }
    pub(crate) fn is_closed(&self) -> bool {
        self.0.budget.state.lock().unwrap().groups[&self.0.id].closed
    }
    pub(crate) fn close(&self) {
        self.0
            .budget
            .state
            .lock()
            .unwrap()
            .groups
            .get_mut(&self.0.id)
            .unwrap()
            .closed = true;
        self.0.budget.changed.notify_waiters();
    }
}

pub(crate) struct CapacityPermit {
    group: CapacityGroup,
}
impl CapacityPermit {
    pub(crate) fn budget(&self) -> Arc<ConnectionBudget> {
        self.group.budget()
    }
}
impl Drop for CapacityPermit {
    fn drop(&mut self) {
        let budget = &self.group.0.budget;
        let mut state = budget.state.lock().unwrap();
        state.used -= 1;
        state.groups.get_mut(&self.group.0.id).unwrap().used -= 1;
        drop(state);
        budget.changed.notify_waiters();
    }
}

#[cfg(all(test, feature = "snowflake"))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_groups_respect_total_and_cancelled_waiters_do_not_reserve_slots() {
        let budget = ConnectionBudget::new(2).unwrap();
        let groups = [budget.group(2).unwrap(), budget.group(2).unwrap()];
        let active = Arc::new(AtomicUsize::new(0));
        let mut tasks = tokio::task::JoinSet::new();
        for i in 0..100 {
            let group = groups[i % 2].clone();
            let active = active.clone();
            tasks.spawn(async move {
                let permit = group
                    .acquire(Instant::now() + Duration::from_secs(5))
                    .await
                    .unwrap();
                assert!(active.fetch_add(1, Ordering::SeqCst) < 2);
                tokio::task::yield_now().await;
                active.fetch_sub(1, Ordering::SeqCst);
                drop(permit);
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        let held = [
            groups[0].try_acquire().unwrap(),
            groups[1].try_acquire().unwrap(),
        ];
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                groups[0].acquire(Instant::now() + Duration::from_secs(1))
            )
            .await
            .is_err()
        );
        drop(held);
        let a = groups[0].try_acquire().unwrap();
        let _b = groups[1].try_acquire().unwrap();
        let waiting = groups[0].acquire(Instant::now() + Duration::from_secs(1));
        tokio::pin!(waiting);
        assert!(futures::poll!(&mut waiting).is_pending());
        groups[0].close();
        assert!(matches!(waiting.await, Err(CapacityError::Closed)));
        assert!(matches!(groups[1].try_acquire(), Err(CapacityError::Full)));
        drop(a);
        assert!(groups[1].try_acquire().is_ok());
    }
}
