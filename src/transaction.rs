use super::{Error, Sequencer, Storage};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{AcqRel, Acquire};

/// A transaction is the atomic unit of work for all types of the storage operations.
///
/// A single strand of change records constitues a single transaction.
/// An on-going transaction can be rewound at a certain point of time.
pub struct Transaction<'s, S: Sequencer> {
    /// The transaction refers to the storage instance to persist the changes at commit.
    storage: &'s Storage<S>,
    /// The transaction refers to the sequencer instance in order to assign a sequence number for commit.
    sequencer: &'s S,
    /// The transaction local clock value is used to differentiate changes made within the transaction.
    transaction_local_clock: AtomicUsize,
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
            transaction_local_clock: AtomicUsize::new(0),
            snapshot_version: None,
        }
    }

    /// Gets the current logical clock value of the transaction.
    ///
    /// The given chunk of log records are kept until the transaction ends.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let transaction = storage.transaction();
    /// assert_eq!(transaction.clock(), 0);
    pub fn clock(&self) -> usize {
        self.transaction_local_clock.load(Acquire)
    }

    /// Advances the logical clock of the transaction by one by feeding log records.
    ///
    /// The given chunk of log records are kept until the transaction ends.
    ///
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let mut transaction = storage.transaction();
    /// let data = [0; 8];
    /// assert_eq!(transaction.advance(&data), 0);
    /// ```
    pub fn advance(&mut self, data: &[u8]) -> usize {
        // An acquire fence is required as the value is used to synchronize transactional changes
        // among threads.
        self.transaction_local_clock.fetch_add(1, AcqRel)
    }

    /// Rewinds the transaction to the given point of time.
    ///
    /// All the changes made between the latest operation sequence and the given one are reverted.
    /// # Examples
    /// ```
    /// use tss::{DefaultSequencer, Storage, Transaction};
    ///
    /// let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
    /// let mut transaction = storage.transaction();
    /// transaction.rewind(0);
    /// ```
    pub fn rewind(&mut self, sequence: usize) -> Result<usize, Error> {
        Err(Error::Fail)
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
