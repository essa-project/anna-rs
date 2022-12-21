use super::Lattice;
use std::collections::HashMap;

/// [`Lattice`] implementation that merges elements by taking their maximum.
///
/// ## Examples
///
/// The merge operation results in the maximum:
///
/// ```
/// use anna::lattice::{Lattice, MaxLattice};
///
/// let mut lattice = MaxLattice::new(4);
/// assert_eq!(lattice.reveal(), &4);
///
/// lattice.merge_element(&6);
/// assert_eq!(lattice.reveal(), &6);
///
/// lattice.merge_element(&5);
/// assert_eq!(lattice.reveal(), &6);
/// ```
///
/// The `MaxLattice` type implements the [`Add`][ops::Add] and [`Sub`][ops::Sub] traits, so it
/// can be manipulated through the `+` and `-` operators:
///
/// ```
/// use anna::lattice::{Lattice, MaxLattice};
///
/// let mut lattice = MaxLattice::new(48);
/// assert_eq!(lattice.reveal(), &48);
///
/// assert_eq!(lattice.clone() + 20, MaxLattice::new(68));
/// assert_eq!((lattice - 37).reveal(), &11);
///
/// // the `+=` and `-=` operators for in-place modification are supported too
/// let mut lattice = MaxLattice::new(2);
/// lattice += 6;
/// assert_eq!(lattice.reveal(), &8);
/// lattice -= 8;
/// assert_eq!(lattice.reveal(), &0);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct CounterLattice {
    element: HashMap<String, ValuePair>,
}

impl CounterLattice {
    /// Constructs a new lattice from the given value.
    pub fn new() -> Self {
        Self {
            element: HashMap::new(),
        }
    }

    /// Gets the value of the counter.
    pub fn total(&self) -> i64 {
        let mut i = 0;
        for (_, v) in &self.element {
            i += v.value
        }
        i
    }

    /// Increments the counter by the given value.
    pub fn inc(&mut self, thread_id: String, clock: usize, value: i64) {
        self.element
            .entry(thread_id)
            .and_modify(|v| {
                v.clock = clock;
                v.value += value;
            })
            .or_insert(ValuePair { clock, value });
    }
}

impl Lattice for CounterLattice {
    type Element = HashMap<String, ValuePair>;

    fn reveal(&self) -> &Self::Element {
        &self.element
    }

    fn reveal_mut(&mut self) -> &mut Self::Element {
        &mut self.element
    }

    fn into_revealed(self) -> Self::Element {
        self.element
    }

    fn merge_element(&mut self, element: &Self::Element) {
        use std::collections::hash_map::Entry;
        for (thread, other_pair) in element.clone() {
            match self.element.entry(thread) {
                Entry::Occupied(mut entry) => {
                    let pair = entry.get_mut();
                    if pair.clock <= other_pair.clock {
                        *pair = other_pair;
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(other_pair);
                }
            }
        }
    }

    fn assign(&mut self, element: Self::Element) {
        self.element = element;
    }
}

/// Pair of a [`VectorClock`] and a value.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[allow(missing_docs)]
pub struct ValuePair {
    pub clock: usize,
    pub value: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_by_value() {
        let mut ml = CounterLattice::default();
        assert_eq!(ml.total(), 0);

        let mut element = HashMap::new();
        element.insert(
            "node-0".to_string(),
            ValuePair {
                clock: 1,
                value: 10,
            },
        );
        ml.merge_element(&element);
        assert_eq!(ml.total(), 10);

        let mut element = HashMap::new();
        element.insert(
            "node-0".to_string(),
            ValuePair {
                clock: 0,
                value: 11,
            },
        );
        ml.merge_element(&element);
        assert_eq!(ml.total(), 10);

        let mut element = HashMap::new();
        element.insert(
            "node-0".to_string(),
            ValuePair {
                clock: 2,
                value: 20,
            },
        );
        element.insert(
            "node-1".to_string(),
            ValuePair {
                clock: 0,
                value: 10,
            },
        );
        ml.merge_element(&element);
        assert_eq!(ml.total(), 30);
    }

    #[test]
    fn merge_by_lattice() {
        let mut ml = CounterLattice::default();
        assert_eq!(ml.total(), 0);

        let mut other = CounterLattice::default();
        other.inc("node-0".to_string(), 0, 10);
        ml.merge(&other);
        assert_eq!(ml.total(), 10);

        let mut other = CounterLattice::default();
        other.inc("node-0".to_string(), 1, 20);
        other.inc("node-1".to_string(), 0, 10);

        ml.merge(&other);
        assert_eq!(ml.total(), 30);
    }

    #[test]
    fn inc() {
        let mut ml = CounterLattice::default();
        assert_eq!(ml.total(), 0);
        ml.inc("node-0".to_string(), 1, 5);
        assert_eq!(ml.total(), 5);
        ml.inc("node-0".to_string(), 1, -7);
        assert_eq!(ml.total(), -2);
    }
}
