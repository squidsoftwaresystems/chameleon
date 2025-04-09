use std::collections::BTreeMap;

/// A struct that bijectively maps from internal `usize` ids
/// to external ids of type `T`
#[derive(PartialEq, Eq)]
pub struct CounterMapper<T: Clone + Ord + Eq> {
    counter: usize,
    map: BTreeMap<usize, T>,
    reverse_map: BTreeMap<T, usize>,
}

impl<T: Clone + Ord + Eq> CounterMapper<T> {
    pub fn new() -> Self {
        Self {
            counter: 0,
            map: BTreeMap::new(),
            reverse_map: BTreeMap::new(),
        }
    }

    /// Add a new item `new_item` if needed.
    /// return the index of the item
    pub fn add_or_find(&mut self, new_item: &T) -> usize {
        if let Some(index) = self.reverse_map.get(new_item) {
            *index
        } else {
            let index = self.counter;
            self.counter += 1;
            self.map.insert(index, new_item.clone());
            self.reverse_map.insert(new_item.clone(), index);

            index
        }
    }

    pub fn map(&self, index: usize) -> Option<T> {
        self.map.get(&index).cloned()
    }

    pub fn reverse_map(&self, item: &T) -> Option<usize> {
        self.reverse_map.get(item).cloned()
    }
}
