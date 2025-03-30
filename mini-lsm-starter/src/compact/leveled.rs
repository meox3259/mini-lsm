// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        _snapshot: &LsmStorageState,
        _sst_ids: &[usize],
        _in_level: usize,
    ) -> Vec<usize> {
        let first_key = _sst_ids
            .iter()
            .map(|x| _snapshot.sstables[x].first_key())
            .min()
            .cloned()
            .unwrap();
        let last_key = _sst_ids
            .iter()
            .map(|x| _snapshot.sstables[x].last_key())
            .max()
            .cloned()
            .unwrap();
        let mut overlapping_ssts = Vec::new();
        for sst in _snapshot.levels[_in_level - 1].1.iter() {
            let _first_key = _snapshot.sstables[sst].first_key();
            let _last_key = _snapshot.sstables[sst].last_key();
            if !(&first_key > _last_key || &last_key < _first_key) {
                overlapping_ssts.push(sst.clone());
            }
        }
        overlapping_ssts
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let mut target_level_size = (0..self.options.max_levels).map(|_| 0).collect::<Vec<_>>();
        let mut real_level_size = Vec::with_capacity(self.options.max_levels);

        for i in 0..self.options.max_levels {
            real_level_size.push(
                _snapshot.levels[i]
                    .1
                    .iter()
                    .map(|x| _snapshot.sstables.get(x).unwrap().table_size())
                    .sum::<u64>(),
            );
        }

        let mut base_level = self.options.max_levels;
        let base_level_size = self.options.base_level_size_mb * 1024 * 1024;
        target_level_size[self.options.max_levels - 1] =
            real_level_size[self.options.max_levels - 1].max(base_level_size as u64);
        for i in (0..(self.options.max_levels - 1)).rev() {
            let next_level_size = target_level_size[i + 1];
            if next_level_size > base_level_size as u64 {
                target_level_size[i] = next_level_size / self.options.level_size_multiplier as u64;
            }
            if target_level_size[i] > 0 {
                base_level = i + 1;
            }
        }

        if _snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    _snapshot,
                    &_snapshot.l0_sstables,
                    base_level,
                ),
                is_lower_level_bottom_level: base_level == self.options.max_levels,
            });
        }

        let mut priority = Vec::new();
        for i in 0..self.options.max_levels {
            if real_level_size[i] as f64 / target_level_size[i] as f64 > 1. {
                priority.push((
                    i + 1,
                    real_level_size[i] as f64 / target_level_size[i] as f64,
                ));
            }
        }

        priority.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        let Some(prior) = priority.first() else {
            return None;
        };
        println!("prior.0: {:?}", prior.0);
        let upper_level_sst = vec![_snapshot.levels[prior.0 - 1]
            .1
            .iter()
            .min()
            .unwrap()
            .clone()];
        Some(LeveledCompactionTask {
            upper_level: Some(prior.0),
            upper_level_sst_ids: upper_level_sst.clone(),
            lower_level: prior.0 + 1,
            lower_level_sst_ids: self.find_overlapping_ssts(
                _snapshot,
                &upper_level_sst,
                prior.0 + 1,
            ),
            is_lower_level_bottom_level: prior.0 + 1 == self.options.max_levels,
        })
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &LeveledCompactionTask,
        _output: &[usize],
        _in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let mut marked_upper_ssts = _task
            .upper_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let mut file_to_remove = Vec::new();

        if let Some(upper_level) = _task.upper_level {
            let new_upper_level_ssts = _snapshot.levels[upper_level - 1]
                .1
                .iter()
                .filter_map(|x| {
                    if marked_upper_ssts.remove(x) {
                        None
                    } else {
                        Some(x.clone())
                    }
                })
                .collect();
            assert!(marked_upper_ssts.is_empty());
            snapshot.levels[upper_level - 1].1 = new_upper_level_ssts;
        } else {
            let new_l0_ssts = _snapshot
                .l0_sstables
                .iter()
                .filter_map(|x| {
                    if marked_upper_ssts.remove(x) {
                        None
                    } else {
                        Some(x.clone())
                    }
                })
                .collect();
            assert!(marked_upper_ssts.is_empty());
            snapshot.l0_sstables = new_l0_ssts;
        }

        let mut marked_lower_ssts = _task
            .lower_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let mut new_lower_level_ssts = _snapshot.levels[_task.lower_level - 1]
            .1
            .iter()
            .filter_map(|x| {
                if marked_lower_ssts.remove(x) {
                    None
                } else {
                    Some(x.clone())
                }
            })
            .collect::<Vec<_>>();
        file_to_remove.extend(&_task.lower_level_sst_ids);
        file_to_remove.extend(&_task.upper_level_sst_ids);
        new_lower_level_ssts.extend(_output);
        new_lower_level_ssts.sort_by(|a, b| {
            _snapshot
                .sstables
                .get(a)
                .unwrap()
                .first_key()
                .cmp(snapshot.sstables.get(b).unwrap().first_key())
        });

        snapshot.levels[_task.lower_level - 1].1 = new_lower_level_ssts;

        (snapshot, file_to_remove)
    }
}
