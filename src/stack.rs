#![allow(unsafe_code)]

use std::{
    fmt::{self, Debug},
    sync::atomic::Ordering::{AcqRel, Acquire},
};

use crossbeam_epoch::{unprotected, Atomic, Guard, Owned, Shared};

use crate::debug_delay;

type CompareAndSwapResult<'g, T> =
    Result<Shared<'g, Vec<T>>, (Shared<'g, Vec<T>>, Owned<Vec<T>>)>;

/// A simple lock-free stack, with the ability to atomically
/// append or entirely swap-out entries.
pub struct Stack<T: Send + 'static> {
    head: Atomic<Vec<T>>,
}

impl<T: Send + 'static> Default for Stack<T> {
    fn default() -> Self {
        Self { head: Atomic::null() }
    }
}

impl<T: Send + 'static> Drop for Stack<T> {
    fn drop(&mut self) {
        unsafe {
            let curr = self.head.load(Acquire, unprotected());
            if !curr.as_raw().is_null() {
                drop(curr.into_owned());
            }
        }
    }
}

impl<T> Debug for Stack<T>
where
    T: Clone + Debug + Send + 'static + Sync,
{
    fn fmt(
        &self,
        formatter: &mut fmt::Formatter<'_>,
    ) -> Result<(), fmt::Error> {
        let guard = crossbeam_epoch::pin();
        let (_, head) = self.head(&guard);
        let iter = head.iter();

        formatter.write_str("Stack [")?;
        let mut written = false;
        for node in iter {
            if written {
                formatter.write_str(", ")?;
            }
            formatter.write_str(&*format!("({:?}) ", &node))?;
            node.fmt(formatter)?;
            written = true;
        }
        formatter.write_str("]")?;
        Ok(())
    }
}

impl<T: Clone + Send + Sync + 'static> Stack<T> {
    /// Add an item to the stack, spinning until successful.
    pub fn push(&mut self, inner: T, guard: &Guard) {
        debug_delay();
        let (_, mut head) = self.head(&guard);
        head.push(inner);
    }

    /// Pop the next item off the stack. Returns None if nothing is there.
    #[cfg(test)]
    fn pop(&mut self, guard: &Guard) -> Option<T> {
        debug_delay();
        let (_, mut head) = self.head(&guard);
        head.pop()
    }

    /// compare and swap
    pub fn cas<'g>(
        &self,
        old: Shared<'g, Vec<T>>,
        new: Vec<T>,
        guard: &'g Guard,
    ) -> CompareAndSwapResult<'g, T> {
        debug_delay();
        let res =
            self.head.compare_and_set(old, Owned::from(new), AcqRel, guard);

        match res {
            Ok(success) => {
                if !old.is_null() {
                    unsafe {
                        guard.defer_destroy(old);
                    };
                }
                Ok(success)
            }
            Err(e) => Err((e.current, e.new)),
        }
    }

    /// Returns the current head pointer of the stack, which can
    /// later be used as the key for cas and cap operations.
    pub fn head<'g>(
        &self,
        guard: &'g Guard,
    ) -> (Shared<'g, Vec<T>>, &'g Vec<T>) {
        let ptr = self.head.load(Acquire, guard);

        if ptr.is_null() {
            (ptr, &vec![])
        } else {
            (ptr, ptr.deref())
        }
    }
}

#[test]
fn basic_functionality() {
    use crossbeam_epoch::pin;
    use crossbeam_utils::CachePadded;
    use std::sync::Arc;
    use std::thread;

    let guard = pin();
    let ll = Arc::new(Stack::default());
    assert_eq!(ll.pop(&guard), None);
    ll.push(CachePadded::new(1), &guard);
    let ll2 = Arc::clone(&ll);
    let t = thread::spawn(move || {
        let guard = pin();
        ll2.push(CachePadded::new(2), &guard);
        ll2.push(CachePadded::new(3), &guard);
        ll2.push(CachePadded::new(4), &guard);
        guard.flush();
    });
    t.join().unwrap();
    ll.push(CachePadded::new(5), &guard);
    assert_eq!(ll.pop(&guard), Some(CachePadded::new(5)));
    assert_eq!(ll.pop(&guard), Some(CachePadded::new(4)));
    let ll3 = Arc::clone(&ll);
    let t = thread::spawn(move || {
        let guard = pin();
        assert_eq!(ll3.pop(&guard), Some(CachePadded::new(3)));
        assert_eq!(ll3.pop(&guard), Some(CachePadded::new(2)));
        guard.flush();
    });
    t.join().unwrap();
    assert_eq!(ll.pop(&guard), Some(CachePadded::new(1)));
    let ll4 = Arc::clone(&ll);
    let t = thread::spawn(move || {
        let guard = pin();
        assert_eq!(ll4.pop(&guard), None);
        guard.flush();
    });
    t.join().unwrap();
    drop(ll);
    guard.flush();
    drop(guard);
}
