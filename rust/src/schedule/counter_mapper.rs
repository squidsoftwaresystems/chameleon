use std::{collections::HashMap, hash::Hash};

/// A struct that bijectively maps from internal `usize` ids
/// to external ids of type `T`
pub struct CounterMapper<T: Clone + Hash + Eq> {
    counter: usize,
    map: HashMap<usize, T>,
    reverse_map: HashMap<T, usize>,
}

impl<T: Clone + Hash + Eq> CounterMapper<T> {
    pub fn new() -> Self {
        Self {
            counter: 0,
            map: HashMap::new(),
            reverse_map: HashMap::new(),
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
