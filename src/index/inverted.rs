//! Tag inverted index for fast series lookup.
//!
//! Maps "tagkey=tagvalue" strings to sorted lists of SeriesIds (posting lists).
//! Supports intersection and union of posting lists for compound tag predicates.

use std::collections::HashMap;

use crate::model::{SeriesId, Tags};

/// Inverted index mapping tag key-value pairs to sets of series IDs.
pub struct InvertedIndex {
    postings: HashMap<String, Vec<SeriesId>>,
}

impl InvertedIndex {
    pub fn new() -> Self {
        Self {
            postings: HashMap::new(),
        }
    }

    /// Index a series by its tags. Each tag key-value pair gets a posting entry.
    pub fn index_series(&mut self, series_id: SeriesId, tags: &Tags) {
        for (key, value) in tags {
            let term = format!("{}={}", key, value);
            let list = self.postings.entry(term).or_default();
            // Insert in sorted order, skip if already present
            match list.binary_search(&series_id) {
                Ok(_) => {} // already indexed
                Err(pos) => list.insert(pos, series_id),
            }
        }
    }

    /// Look up all series IDs that have a specific tag key-value pair.
    pub fn lookup(&self, tag_key: &str, tag_value: &str) -> &[SeriesId] {
        let term = format!("{}={}", tag_key, tag_value);
        self.postings.get(&term).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Intersect multiple posting lists (AND semantics).
    ///
    /// Returns series IDs that appear in *all* input lists.
    pub fn intersect(lists: &[&[SeriesId]]) -> Vec<SeriesId> {
        if lists.is_empty() {
            return Vec::new();
        }
        if lists.len() == 1 {
            return lists[0].to_vec();
        }

        // Start with the shortest list for efficiency
        let mut sorted_lists: Vec<&[SeriesId]> = lists.to_vec();
        sorted_lists.sort_by_key(|l| l.len());

        let mut result: Vec<SeriesId> = sorted_lists[0].to_vec();

        for list in &sorted_lists[1..] {
            result = Self::sorted_intersect(&result, list);
            if result.is_empty() {
                break;
            }
        }

        result
    }

    /// Union multiple posting lists (OR semantics).
    ///
    /// Returns series IDs that appear in *any* input list.
    pub fn union(lists: &[&[SeriesId]]) -> Vec<SeriesId> {
        if lists.is_empty() {
            return Vec::new();
        }
        if lists.len() == 1 {
            return lists[0].to_vec();
        }

        let mut result = lists[0].to_vec();
        for list in &lists[1..] {
            result = Self::sorted_union(&result, list);
        }
        result
    }

    fn sorted_intersect(a: &[SeriesId], b: &[SeriesId]) -> Vec<SeriesId> {
        let mut result = Vec::new();
        let (mut i, mut j) = (0, 0);
        while i < a.len() && j < b.len() {
            match a[i].cmp(&b[j]) {
                std::cmp::Ordering::Equal => {
                    result.push(a[i]);
                    i += 1;
                    j += 1;
                }
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
            }
        }
        result
    }

    fn sorted_union(a: &[SeriesId], b: &[SeriesId]) -> Vec<SeriesId> {
        let mut result = Vec::with_capacity(a.len() + b.len());
        let (mut i, mut j) = (0, 0);
        while i < a.len() && j < b.len() {
            match a[i].cmp(&b[j]) {
                std::cmp::Ordering::Equal => {
                    result.push(a[i]);
                    i += 1;
                    j += 1;
                }
                std::cmp::Ordering::Less => {
                    result.push(a[i]);
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    result.push(b[j]);
                    j += 1;
                }
            }
        }
        result.extend_from_slice(&a[i..]);
        result.extend_from_slice(&b[j..]);
        result
    }
}

impl Default for InvertedIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn tags(pairs: &[(&str, &str)]) -> Tags {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn index_and_lookup() {
        let mut idx = InvertedIndex::new();
        idx.index_series(SeriesId(1), &tags(&[("host", "a"), ("region", "us")]));
        idx.index_series(SeriesId(2), &tags(&[("host", "b"), ("region", "us")]));
        idx.index_series(SeriesId(3), &tags(&[("host", "a"), ("region", "eu")]));

        assert_eq!(idx.lookup("host", "a"), &[SeriesId(1), SeriesId(3)]);
        assert_eq!(idx.lookup("host", "b"), &[SeriesId(2)]);
        assert_eq!(idx.lookup("region", "us"), &[SeriesId(1), SeriesId(2)]);
        assert_eq!(idx.lookup("region", "eu"), &[SeriesId(3)]);
    }

    #[test]
    fn lookup_missing_returns_empty() {
        let idx = InvertedIndex::new();
        assert!(idx.lookup("host", "a").is_empty());
    }

    #[test]
    fn duplicate_indexing_is_idempotent() {
        let mut idx = InvertedIndex::new();
        let t = tags(&[("host", "a")]);
        idx.index_series(SeriesId(1), &t);
        idx.index_series(SeriesId(1), &t);
        assert_eq!(idx.lookup("host", "a"), &[SeriesId(1)]);
    }

    #[test]
    fn intersect_two_lists() {
        let a = [SeriesId(1), SeriesId(2), SeriesId(3), SeriesId(5)];
        let b = [SeriesId(2), SeriesId(3), SeriesId(4)];
        let result = InvertedIndex::intersect(&[&a, &b]);
        assert_eq!(result, vec![SeriesId(2), SeriesId(3)]);
    }

    #[test]
    fn intersect_three_lists() {
        let a = [SeriesId(1), SeriesId(2), SeriesId(3), SeriesId(5)];
        let b = [SeriesId(2), SeriesId(3), SeriesId(4)];
        let c = [SeriesId(3), SeriesId(5)];
        let result = InvertedIndex::intersect(&[&a, &b, &c]);
        assert_eq!(result, vec![SeriesId(3)]);
    }

    #[test]
    fn intersect_disjoint() {
        let a = [SeriesId(1), SeriesId(2)];
        let b = [SeriesId(3), SeriesId(4)];
        let result = InvertedIndex::intersect(&[&a, &b]);
        assert!(result.is_empty());
    }

    #[test]
    fn intersect_empty_input() {
        let result = InvertedIndex::intersect(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn intersect_single_list() {
        let a = [SeriesId(1), SeriesId(2)];
        let result = InvertedIndex::intersect(&[&a]);
        assert_eq!(result, vec![SeriesId(1), SeriesId(2)]);
    }

    #[test]
    fn union_two_lists() {
        let a = [SeriesId(1), SeriesId(3), SeriesId(5)];
        let b = [SeriesId(2), SeriesId(3), SeriesId(4)];
        let result = InvertedIndex::union(&[&a, &b]);
        assert_eq!(
            result,
            vec![SeriesId(1), SeriesId(2), SeriesId(3), SeriesId(4), SeriesId(5)]
        );
    }

    #[test]
    fn union_empty() {
        let result = InvertedIndex::union(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn union_single_list() {
        let a = [SeriesId(1), SeriesId(2)];
        let result = InvertedIndex::union(&[&a]);
        assert_eq!(result, vec![SeriesId(1), SeriesId(2)]);
    }

    #[test]
    fn end_to_end_tag_query() {
        let mut idx = InvertedIndex::new();
        // Simulate: find all series where host=a AND region=us
        idx.index_series(SeriesId(1), &tags(&[("host", "a"), ("region", "us")]));
        idx.index_series(SeriesId(2), &tags(&[("host", "b"), ("region", "us")]));
        idx.index_series(SeriesId(3), &tags(&[("host", "a"), ("region", "eu")]));
        idx.index_series(SeriesId(4), &tags(&[("host", "a"), ("region", "us")]));

        let host_a = idx.lookup("host", "a");
        let region_us = idx.lookup("region", "us");
        let result = InvertedIndex::intersect(&[host_a, region_us]);
        assert_eq!(result, vec![SeriesId(1), SeriesId(4)]);
    }
}
