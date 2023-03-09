//! A key value store implementation for lattice types.

use eyre::anyhow;

use crate::{
    lattice::{
        causal::{
            MultiKeyCausalLattice, MultiKeyCausalPayload, SingleKeyCausalLattice,
            VectorClockValuePair,
        },
        last_writer_wins::TimestampValuePair,
        CounterLattice, LastWriterWinsLattice, Lattice, MapLattice, MaxLattice, OrderedSetLattice,
        SetLattice,
    },
    AnnaError,
};
// pub use anna_api::LatticeValue;
use std::{
    borrow::Borrow,
    collections::{hash_map, HashMap},
    hash::Hash,
    mem::{self, size_of_val},
};

/// Describes a value stored in the key value store.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LatticeValue {
    /// Last-writer wins lattice
    Lww(LastWriterWinsLattice<Vec<u8>>),
    /// Unordered set lattice
    Set(SetLattice<Vec<u8>>),
    /// Ordered-set lattice
    OrderedSet(OrderedSetLattice<Vec<u8>>),
    /// Single-key causal Counter lattice
    Counter(CounterLattice),
    /// Single-key causal Set lattice
    SingleCausal(SingleKeyCausalLattice<SetLattice<Vec<u8>>>),
    /// Single-key causal Map lattice
    SingleCausalMap(SingleKeyCausalLattice<MapLattice<String, LastWriterWinsLattice<Vec<u8>>>>),
    /// Multi-key causal lattice
    MultiCausal(MultiKeyCausalLattice<SetLattice<Vec<u8>>>),
}

impl LatticeValue {
    /// Tries to cast the value to an [`LastWriterWinsLattice`].
    ///
    /// Errors if the value is of a different type.
    pub fn as_lww(&self) -> eyre::Result<&LastWriterWinsLattice<Vec<u8>>> {
        match self {
            Self::Lww(val) => Ok(val),
            other => Err(anyhow!("expected Lww lattice, got `{:?}`", other)),
        }
    }

    /// Tries to cast the value to an [`LastWriterWinsLattice`], taking ownership.
    ///
    /// Errors if the value is of a different type.
    pub fn into_lww(self) -> eyre::Result<LastWriterWinsLattice<Vec<u8>>> {
        match self {
            Self::Lww(val) => Ok(val),
            other => Err(anyhow!("expected Lww lattice, got `{:?}`", other)),
        }
    }

    /// Tries to cast the value to a [`SetLattice`].
    ///
    /// Errors if the value is of a different type.
    pub fn as_set(&self) -> eyre::Result<&SetLattice<Vec<u8>>> {
        match self {
            Self::Set(val) => Ok(val),
            other => Err(anyhow!("expected Set lattice, got `{:?}`", other)),
        }
    }

    /// Tries to cast the value to a [`SetLattice`], taking ownership.
    ///
    /// Errors if the value is of a different type.
    pub fn into_set(self) -> eyre::Result<SetLattice<Vec<u8>>> {
        match self {
            Self::Set(val) => Ok(val),
            other => Err(anyhow!("expected Set lattice, got `{:?}`", other)),
        }
    }

    /// Tries to cast the value to an [`OrderedSetLattice`].
    ///
    /// Errors if the value is of a different type.
    pub fn as_ordered_set(&self) -> eyre::Result<&OrderedSetLattice<Vec<u8>>> {
        match self {
            Self::OrderedSet(val) => Ok(val),
            other => Err(anyhow!("expected OrderedSet lattice, got `{:?}`", other)),
        }
    }

    /// Tries to cast the value to an [`OrderedSetLattice`], taking ownership.
    ///
    /// Errors if the value is of a different type.
    pub fn into_ordered_set(self) -> eyre::Result<OrderedSetLattice<Vec<u8>>> {
        match self {
            Self::OrderedSet(val) => Ok(val),
            other => Err(anyhow!("expected OrderedSet lattice, got `{:?}`", other)),
        }
    }

    /// Tries to cast the value to a [`SingleKeyCausalLattice`].
    ///
    /// Errors if the value is of a different type.
    pub fn as_single_causal(&self) -> eyre::Result<&SingleKeyCausalLattice<SetLattice<Vec<u8>>>> {
        match self {
            Self::SingleCausal(val) => Ok(val),
            other => Err(anyhow!("expected SingleCausal lattice, got `{:?}`", other)),
        }
    }

    /// Tries to cast the value to a [`SingleKeyCausalLattice`], taking ownership.
    ///
    /// Errors if the value is of a different type.
    pub fn into_single_causal(self) -> eyre::Result<SingleKeyCausalLattice<SetLattice<Vec<u8>>>> {
        match self {
            Self::SingleCausal(val) => Ok(val),
            other => Err(anyhow!("expected SingleCausal lattice, got `{:?}`", other)),
        }
    }

    /// Tries to cast the value to a [`MultiKeyCausalLattice`].
    ///
    /// Errors if the value is of a different type.
    pub fn as_multi_causal(&self) -> eyre::Result<&MultiKeyCausalLattice<SetLattice<Vec<u8>>>> {
        match self {
            Self::MultiCausal(val) => Ok(val),
            other => Err(anyhow!("expected MultiCausal lattice, got `{:?}`", other)),
        }
    }

    /// Tries to cast the value to a [`MultiKeyCausalLattice`], taking ownership.
    ///
    /// Errors if the value is of a different type.
    pub fn into_multi_causal(self) -> eyre::Result<MultiKeyCausalLattice<SetLattice<Vec<u8>>>> {
        match self {
            Self::MultiCausal(val) => Ok(val),
            other => Err(anyhow!("expected MultiCausal lattice, got `{:?}`", other)),
        }
    }

    /// Merges the given value into `self` if both values are of the same lattice type.
    ///
    /// If the given value is of a different lattice type than `self`, an `AnnaError::Lattice`
    /// is returned.
    pub fn try_merge(&mut self, other: &LatticeValue) -> Result<(), AnnaError> {
        match (self, other) {
            (LatticeValue::Lww(s), LatticeValue::Lww(other)) => {
                s.merge(other);
                Ok(())
            }
            (LatticeValue::Set(s), LatticeValue::Set(other)) => {
                s.merge(other);
                Ok(())
            }
            (LatticeValue::OrderedSet(s), LatticeValue::OrderedSet(other)) => {
                s.merge(other);
                Ok(())
            }
            (LatticeValue::SingleCausal(s), LatticeValue::SingleCausal(other)) => {
                s.merge(other);
                Ok(())
            }
            (LatticeValue::SingleCausalMap(s), LatticeValue::SingleCausalMap(other)) => {
                s.merge(other);
                Ok(())
            }
            (LatticeValue::MultiCausal(s), LatticeValue::MultiCausal(other)) => {
                s.merge(other);
                Ok(())
            }
            (x, y) if x.ty() == y.ty() => unreachable!(),
            _ => Err(AnnaError::Lattice),
        }
    }

    /// Returns the lattice type of the value.
    pub fn ty(&self) -> LatticeType {
        match self {
            LatticeValue::Lww(_) => LatticeType::Lww,
            LatticeValue::Set(_) => LatticeType::Set,
            LatticeValue::OrderedSet(_) => LatticeType::OrderedSet,
            LatticeValue::Counter(_) => LatticeType::Counter,
            LatticeValue::SingleCausal(_) => LatticeType::SingleCausal,
            LatticeValue::SingleCausalMap(_) => LatticeType::SingleCausalMap,
            LatticeValue::MultiCausal(_) => LatticeType::MultiCausal,
        }
    }
}

impl From<MultiKeyCausalLattice<SetLattice<Vec<u8>>>> for LatticeValue {
    fn from(v: MultiKeyCausalLattice<SetLattice<Vec<u8>>>) -> Self {
        Self::MultiCausal(v)
    }
}

impl From<SingleKeyCausalLattice<SetLattice<Vec<u8>>>> for LatticeValue {
    fn from(v: SingleKeyCausalLattice<SetLattice<Vec<u8>>>) -> Self {
        Self::SingleCausal(v)
    }
}

impl From<OrderedSetLattice<Vec<u8>>> for LatticeValue {
    fn from(v: OrderedSetLattice<Vec<u8>>) -> Self {
        Self::OrderedSet(v)
    }
}

impl From<SetLattice<Vec<u8>>> for LatticeValue {
    fn from(v: SetLattice<Vec<u8>>) -> Self {
        Self::Set(v)
    }
}

impl From<LastWriterWinsLattice<Vec<u8>>> for LatticeValue {
    fn from(val: LastWriterWinsLattice<Vec<u8>>) -> Self {
        Self::Lww(val)
    }
}

/// Defines the different types of lattices that we support.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum LatticeType {
    /// Last-writer wins lattice
    Lww,
    /// Unordered set lattice
    Set,
    /// Counter lattice
    Counter,
    /// Single-key causal lattice
    SingleCausal,
    /// Single-key causal map lattice
    SingleCausalMap,
    /// Multi-key causal lattice
    MultiCausal,
    /// Ordered-set lattice
    OrderedSet,
    /// Priority lattice
    Priority,
}

/// A key-value store for lattice values.
pub struct LatticeValueStore<K> {
    db: HashMap<K, LatticeValue>,
}

impl<K> LatticeValueStore<K>
where
    K: Hash + Eq,
{
    /// Gets the current value for the given key, if present.
    pub fn get<Q>(&self, key: &Q) -> Option<&LatticeValue>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.db.get(key)
    }

    /// Inserts or updates the map with the given key/value combination.
    ///
    /// If the value is already present in the map, the [`Lattice::merge`] function
    /// is used to merge the old and new values. This requires that the two values are
    /// of the same lattice type, i.e. the same variant of the [`LatticeValue`] enum.
    /// If this is not the case, an [`AnnaError::Lattice`] is returned.
    pub fn put(&mut self, key: K, value: LatticeValue) -> Result<(), AnnaError> {
        match self.db.entry(key) {
            hash_map::Entry::Vacant(entry) => {
                entry.insert(value);
                Ok(())
            }
            hash_map::Entry::Occupied(mut entry) => entry.get_mut().try_merge(&value),
        }
    }

    /// Same as `std::collections::HashMap::entry`.
    pub(crate) fn entry<'db>(&'db mut self, key: K) -> hash_map::Entry<'db, K, LatticeValue> {
        self.db.entry(key)
    }

    /// Removes the given key from the store.
    pub fn remove<Q>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.db.remove(key);
    }

    /// Returns an [`Iterator`] of all keys in the store.
    pub fn keys(&self) -> hash_map::Keys<K, LatticeValue> {
        self.db.keys()
    }

    /// Returns whether the store contains the given key.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.db.contains_key(key)
    }

    /// An iterator visiting all stored key-value pairs in arbitary order.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &LatticeValue)> {
        self.db.iter()
    }
}

impl<K> Default for LatticeValueStore<K> {
    fn default() -> Self {
        Self {
            db: Default::default(),
        }
    }
}

/// Estimates the size of a lattice to estimate the storage consumption of stored values.
///
/// This is still a work in progress implementation.
pub trait LatticeSizeEstimate {
    /// Returns an estimate of the size of the lattice.
    fn size_estimate(&self) -> usize;
}

impl LatticeSizeEstimate for LatticeValue {
    fn size_estimate(&self) -> usize {
        match self {
            LatticeValue::Lww(lattice) => lattice.size_estimate(),
            LatticeValue::Set(lattice) => lattice.size_estimate(),
            LatticeValue::OrderedSet(lattice) => lattice.size_estimate(),
            LatticeValue::Counter(lattice) => lattice.size_estimate(),
            LatticeValue::SingleCausal(lattice) => lattice.size_estimate(),
            LatticeValue::SingleCausalMap(lattice) => lattice.size_estimate(),
            LatticeValue::MultiCausal(lattice) => lattice.size_estimate(),
        }
    }
}

impl<T> LatticeSizeEstimate for LastWriterWinsLattice<T>
where
    TimestampValuePair<T>: LatticeSizeEstimate,
{
    fn size_estimate(&self) -> usize {
        self.element().size_estimate()
    }
}

impl LatticeSizeEstimate for TimestampValuePair<Vec<u8>> {
    fn size_estimate(&self) -> usize {
        self.value().len() + mem::size_of_val(&self.timestamp())
    }
}

impl<V> LatticeSizeEstimate for MapLattice<String, V>
where
    V: LatticeSizeEstimate + Lattice + Clone,
{
    fn size_estimate(&self) -> usize {
        let mut size = 0;
        for (key, value) in self.reveal() {
            size += key.len();
            size += value.size_estimate();
        }
        size
    }
}

impl<V> LatticeSizeEstimate for MapLattice<char, V>
where
    V: LatticeSizeEstimate + Lattice + Clone,
{
    fn size_estimate(&self) -> usize {
        let mut size = 0;
        for (key, value) in self.reveal() {
            size += std::mem::size_of_val(key);
            size += value.size_estimate();
        }
        size
    }
}

impl LatticeSizeEstimate for MaxLattice<usize> {
    fn size_estimate(&self) -> usize {
        std::mem::size_of::<usize>()
    }
}

impl LatticeSizeEstimate for SetLattice<char> {
    fn size_estimate(&self) -> usize {
        self.reveal().len() * mem::size_of::<char>()
    }
}

impl LatticeSizeEstimate for SetLattice<Vec<u8>> {
    fn size_estimate(&self) -> usize {
        let mut size = 0;
        for val in self.reveal() {
            // add the part that is stored in the hash set
            size += mem::size_of_val(val);
            // the actual vector data lives in a separate heap allocation, so add it too;
            // u8 has size 1, so adding the length is enough
            size += val.len();
        }
        size
    }
}

impl LatticeSizeEstimate for OrderedSetLattice<Vec<u8>> {
    fn size_estimate(&self) -> usize {
        self.reveal().len()
    }
}

impl LatticeSizeEstimate for CounterLattice {
    fn size_estimate(&self) -> usize {
        let mut size = 0;

        for (k, v) in self.reveal() {
            size += mem::size_of_val(k);
            size += mem::size_of_val(v);
        }
        size
    }
}

impl<T> LatticeSizeEstimate for SingleKeyCausalLattice<T>
where
    VectorClockValuePair<T>: LatticeSizeEstimate,
    T: Lattice + Clone,
{
    fn size_estimate(&self) -> usize {
        self.reveal().size_estimate()
    }
}

impl<T> LatticeSizeEstimate for VectorClockValuePair<T>
where
    T: LatticeSizeEstimate,
{
    fn size_estimate(&self) -> usize {
        self.vector_clock.size_estimate() * 2 * mem::size_of::<usize>() + self.value.size_estimate()
    }
}

impl<T> LatticeSizeEstimate for MultiKeyCausalLattice<T>
where
    MultiKeyCausalPayload<T>: LatticeSizeEstimate,
    T: Lattice + Clone,
{
    fn size_estimate(&self) -> usize {
        self.reveal().size_estimate()
    }
}

impl LatticeSizeEstimate for MultiKeyCausalPayload<SetLattice<Vec<u8>>> {
    fn size_estimate(&self) -> usize {
        let mut dep_size = 0;
        for (k, v) in self.dependencies.reveal() {
            dep_size += k.len();
            dep_size += v.size_estimate() * 2 * size_of_val(&dep_size);
        }

        self.vector_clock.size_estimate() * 2 * size_of_val(&dep_size)
            + dep_size
            + self.value.size_estimate()
    }
}

#[cfg(test)]
mod map_tests {
    use super::*;
    use crate::lattice::MaxLattice;
    use std::collections::HashSet;

    #[test]
    fn assign() {
        let map1: HashMap<_, _> = [('a', MaxLattice::new(10)), ('b', MaxLattice::new(20))]
            .iter()
            .cloned()
            .collect();

        let mut mapl = MapLattice::default();
        assert_eq!(mapl.size_estimate(), 0);

        mapl.assign(map1.clone());
        assert_eq!(mapl.size_estimate(), 24);
        assert_eq!(mapl.reveal(), &map1)
    }

    #[test]
    fn merge_by_value() {
        let map1: HashMap<_, _> = [('a', MaxLattice::new(10)), ('b', MaxLattice::new(20))]
            .iter()
            .cloned()
            .collect();
        let map2: HashMap<_, _> = [('b', MaxLattice::new(30)), ('c', MaxLattice::new(40))]
            .iter()
            .cloned()
            .collect();
        let map3: HashMap<_, _> = [
            ('a', MaxLattice::new(10)),
            ('b', MaxLattice::new(30)),
            ('c', MaxLattice::new(40)),
        ]
        .iter()
        .cloned()
        .collect();

        let mut mapl = MapLattice::default();
        assert_eq!(mapl.size_estimate(), 0);

        mapl.merge_element(&map1);
        assert_eq!(mapl.size_estimate(), 24);
        assert_eq!(mapl.reveal(), &map1);

        mapl.merge_element(&map2);
        assert_eq!(mapl.size_estimate(), 36);
        assert_eq!(mapl.reveal(), &map3);
    }

    #[test]
    fn merge_by_lattice() {
        let map1: HashMap<_, _> = [('a', MaxLattice::new(10)), ('b', MaxLattice::new(20))]
            .iter()
            .cloned()
            .collect();
        let map2: HashMap<_, _> = [('b', MaxLattice::new(30)), ('c', MaxLattice::new(40))]
            .iter()
            .cloned()
            .collect();
        let map3: HashMap<_, _> = [
            ('a', MaxLattice::new(10)),
            ('b', MaxLattice::new(30)),
            ('c', MaxLattice::new(40)),
        ]
        .iter()
        .cloned()
        .collect();

        let mut mapl = MapLattice::default();
        assert_eq!(mapl.size_estimate(), 0);

        mapl.merge(&MapLattice::new(map1.clone()));
        assert_eq!(mapl.size_estimate(), 24);
        assert_eq!(mapl.reveal(), &map1);

        mapl.merge(&MapLattice::new(map2));
        assert_eq!(mapl.size_estimate(), 36);
        assert_eq!(mapl.reveal(), &map3);
    }

    #[test]
    fn key_set() {
        let map1: HashMap<_, _> = [('a', MaxLattice::new(10)), ('b', MaxLattice::new(20))]
            .iter()
            .cloned()
            .collect();

        let mut mapl = MapLattice::default();
        assert_eq!(mapl.size_estimate(), 0);

        mapl.merge_element(&map1);
        let res = mapl.key_set();
        assert_eq!(
            res.reveal(),
            &['a', 'b'].iter().copied().collect::<HashSet<_>>()
        );
    }

    #[test]
    fn at() {
        let map1: HashMap<_, _> = [('a', MaxLattice::new(10)), ('b', MaxLattice::new(20))]
            .iter()
            .cloned()
            .collect();

        let mut mapl = MapLattice::default();
        assert_eq!(mapl.size_estimate(), 0);

        mapl.merge_element(&map1);
        let res = mapl.reveal().get(&'a').unwrap();
        assert_eq!(res.reveal(), &10);
    }

    #[test]
    fn contains() {
        let map1: HashMap<_, _> = [('a', MaxLattice::new(10)), ('b', MaxLattice::new(20))]
            .iter()
            .cloned()
            .collect();

        let mut mapl = MapLattice::default();
        assert_eq!(mapl.size_estimate(), 0);

        mapl.merge_element(&map1);
        let res = mapl.contains_key(&'a');
        assert_eq!(res.reveal(), &true);
        let res = mapl.contains_key(&'d');
        assert_eq!(res.reveal(), &false);
    }
}

#[cfg(test)]
mod set_tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn assign() {
        let set1: HashSet<char> = ['a', 'b', 'c'].iter().copied().collect();
        let mut sl = SetLattice::default();
        assert_eq!(sl.reveal().len(), 0);
        assert_eq!(sl.size_estimate(), 0);
        sl.assign(set1.clone());
        assert_eq!(sl.reveal().len(), 3);
        assert_eq!(sl.size_estimate(), 3 * mem::size_of::<char>());
        assert_eq!(sl.reveal(), &set1);
    }

    #[test]
    fn merge_by_value() {
        let set1: HashSet<char> = ['a', 'b', 'c'].iter().copied().collect();
        let set2: HashSet<char> = ['c', 'd', 'e'].iter().copied().collect();
        let set3: HashSet<char> = ['a', 'd', 'e', 'b', 'c'].iter().copied().collect();

        let mut sl = SetLattice::default();
        assert_eq!(sl.size_estimate(), 0);

        sl.merge_element(&set1);
        assert_eq!(sl.size_estimate(), 3 * mem::size_of::<char>());
        assert_eq!(sl.reveal(), &set1);

        sl.merge_element(&set2);
        assert_eq!(sl.size_estimate(), 5 * mem::size_of::<char>());
        assert_eq!(sl.reveal(), &set3);
    }

    #[test]
    fn merge_by_lattice() {
        let set1: HashSet<char> = ['a', 'b', 'c'].iter().copied().collect();
        let set2: HashSet<char> = ['c', 'd', 'e'].iter().copied().collect();
        let set3: HashSet<char> = ['a', 'd', 'e', 'b', 'c'].iter().copied().collect();

        let mut sl = SetLattice::default();
        assert_eq!(sl.size_estimate(), 0);

        sl.merge(&SetLattice::new(set1.clone()));
        assert_eq!(sl.size_estimate(), 3 * mem::size_of::<char>());
        assert_eq!(sl.reveal(), &set1);

        sl.merge(&SetLattice::new(set2));
        assert_eq!(sl.size_estimate(), 5 * mem::size_of::<char>());
        assert_eq!(sl.reveal(), &set3);
    }
}
