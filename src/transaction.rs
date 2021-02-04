use super::{Error, Sequencer, Storage};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Acquire;

/// A transaction is the atomic unit of work for all types of the storage operations.
pub struct Transaction<'s, S: Sequencer> {
    storage: &'s Storage<S>,
    sequencer: &'s S,
    /// The operation sequence value is used to differentiate changes made within the transaction.
    current_operation_sequence: AtomicUsize,
    /// The snapshot version is assigned at commit time.
    snapshot_version: Option<S::Clock>,
}

impl<'s, S: Sequencer> Transaction<'s, S> {
    /// Creates a new transaction.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let transaction = storage.transaction();
    /// ```
    pub fn new(storage: &'s Storage<S>, sequencer: &'s S) -> Transaction<'s, S> {
        Transaction {
            storage,
            sequencer,
            current_operation_sequence: AtomicUsize::new(0),
            snapshot_version: None,
        }
    }

    /// Returns the current operation sequence of the transaction.
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let transaction = storage.transaction();
    /// assert_eq!(transaction.sequence(), 0);
    /// ```
    pub fn sequence(&self) -> usize {
        // An acquire fence is required as the value is used to synchronize transactional changes
        // among threads.
        self.current_operation_sequence.load(Acquire)
    }

    /// Commits a transaction.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let mut transaction = storage.transaction();
    /// transaction.commit();
    /// ```
    pub fn commit(mut self) -> Result<Rubicon<'s, S>, Error> {
        // Assigns a new snapshot version.
        self.snapshot_version.replace(self.sequencer.advance());
        Err(Error::Fail)
    }

    /// Rolls back a transaction.
    ///
    /// If there is nothing to rollback, it returns false.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let mut transaction = storage.transaction();
    /// transaction.rollback();
    /// ```
    pub fn rollback(mut self) {
        // Dropping the instance entails a synchronous transaction rollback.
        drop(self);
    }

    /// Post processes transaction commit.
    ///
    /// Only a Rubicon instance is allowed to call this function.
    /// Once a transaction is post-processed, the transaction cannot be rolled back.
    fn post_process(mut self) {}
}

impl<'s, S: Sequencer> Drop for Transaction<'s, S> {
    fn drop(&mut self) {
        // Rolls back the transaction is not committed.
        if self.snapshot_version.is_none() {}
    }
}

/// Rubicon gives one last chance of rolling back the transaction.
///
/// The transaction is bound to be committed if no actions are taken before dropping the Rubicon
/// instance. On the other hands, the transaction stays uncommitted until the Rubicon instance is
/// dropped.
///
/// The fact that the transaction is stopped just before being fully committed enables developers
/// to implement a distributed transaction commit protocols, such as the two-phase-commit protocol,
/// or X/Open XA. One will be able to regard a state where a piece of code holding a Rubicon
/// instance as being a state where the distributed transaction is prepared.
pub struct Rubicon<'s, S: Sequencer> {
    transaction: Option<Transaction<'s, S>>,
}

impl<'s, S: Sequencer> Rubicon<'s, S> {
    /// Rolls back a committable transaction.
    ///
    /// If there is nothing to rollback, it returns false.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let mut transaction = storage.transaction();
    /// if let Ok(rubicon) = transaction.commit() {
    ///     rubicon.rollback();
    /// }
    ///
    /// let mut transaction = storage.transaction();
    /// transaction.commit();
    /// ```
    pub fn rollback(mut self) {
        if let Some(transaction) = self.transaction.take() {
            transaction.rollback();
        }
    }
}

impl<'s, S: Sequencer> Drop for Rubicon<'s, S> {
    /// Post processes the transaction is the transaction is not explicitly rolled back.
    fn drop(&mut self) {
        if let Some(transaction) = self.transaction.take() {
            transaction.post_process();
        }
    }
}
