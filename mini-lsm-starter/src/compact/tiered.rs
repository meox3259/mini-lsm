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
use std::collections::HashMap;

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        let snapshot = _snapshot.clone();
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }
        let mut size = 0;
        for i in 0..(snapshot.levels.len() - 1) {
            size += snapshot.levels[i].1.len();
        }

        // 全部Compaction到最底层，当（最底层以外的大小/最底层大小）> max_size_amplification_percent时
        if size as f64 / snapshot.levels.last().unwrap().1.len() as f64
            > self.options.max_size_amplification_percent as f64 / 100.
        {
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }
        let size_ratio = (100. + self.options.size_ratio as f64) / 100.;
        let mut size = 0;
        for i in 0..(snapshot.levels.len() - 1) {
            size += snapshot.levels[i].1.len();
            if size as f64 / snapshot.levels[i + 1].1.len() as f64 > size_ratio
                && i + 1 >= self.options.min_merge_width
            {
                return Some(TieredCompactionTask {
                    tiers: snapshot
                        .levels
                        .iter()
                        .take(i + 1)
                        .cloned()
                        .collect::<Vec<_>>(),
                    bottom_tier_included: i + 1 >= snapshot.levels.len(),
                });
            }
        }
        let num_tiers_to_compact = snapshot
            .levels
            .len()
            .min(self.options.max_merge_width.unwrap_or(usize::MAX));
        Some(TieredCompactionTask {
            tiers: snapshot
                .levels
                .iter()
                .take(num_tiers_to_compact)
                .cloned()
                .collect::<Vec<_>>(),
            bottom_tier_included: num_tiers_to_compact >= snapshot.levels.len(),
        })
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &TieredCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let mut mark_for_deletion = _task
            .tiers
            .iter()
            .clone()
            .map(|(x, y)| (*x, y.clone()))
            .collect::<HashMap<_, _>>();
        let mut files_to_remove = Vec::new();
        let mut levels = Vec::new();
        let mut add_new_tier = false;
        for (tier, sst_ids) in snapshot.levels.iter() {
            if let Some(ssts) = mark_for_deletion.remove(tier) {
                files_to_remove.extend(ssts);
            } else {
                levels.push((*tier, sst_ids.clone()));
            }
            // 将新的copact的层加到对应的位置
            if mark_for_deletion.is_empty() && !add_new_tier {
                levels.push((_output[0], _output.to_vec()));
                add_new_tier = true;
            }
        }
        snapshot.levels = levels;
        (snapshot, files_to_remove)
    }
}
