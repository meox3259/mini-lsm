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

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod leveled;
mod simple_leveled;
mod tiered;

use crate::key::KeySlice;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact_iter_to_ssts(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut builder = None;
        let mut new_sst = Vec::new();

        while iter.is_valid() {
            if builder.is_none() {
                builder = Some(SsTableBuilder::new(self.options.block_size));
            }

            let builder_inner = builder.as_mut().unwrap();

            // 在当前sst builder里插入一个key
            if compact_to_bottom_level {
                if !iter.value().is_empty() {
                    builder_inner.add(iter.key(), iter.value());
                }
            } else {
                builder_inner.add(iter.key(), iter.value());
            }
            iter.next()?;

            // 如果当前sst大小达到限制，则生成一个新的sst
            if builder_inner.estimated_size() >= self.options.target_sst_size {
                let sst_id = self.next_sst_id();
                // builder清空
                let builder = builder.take().unwrap();
                let sst = Arc::new(builder.build(
                    sst_id,
                    Some(Arc::clone(&self.block_cache)),
                    self.path_of_sst(sst_id),
                )?);
                new_sst.push(sst);
            }
        }

        // 最后一个builder清空
        if let Some(builder) = builder {
            let sst_id = self.next_sst_id();
            let sst = Arc::new(builder.build(
                sst_id,
                Some(Arc::clone(&self.block_cache)),
                self.path_of_sst(sst_id),
            )?);
            new_sst.push(sst);
        }

        Ok(new_sst)
    }

    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let state = self.state.read();
            Arc::clone(&state)
        };
        println!("2222222222222222222222222222222222");
        match _task {
            // l0和l1全部合并
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                // 建立l0层的SsTableIterator
                let mut iter_l0 = Vec::new();
                for sst in l0_sstables.iter() {
                    iter_l0.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        snapshot.sstables.get(sst).unwrap().clone(),
                    )?));
                }

                // 建立l1层有序的SstConcatIterator
                let mut iter_l1 = Vec::new();
                for sst in l1_sstables.iter() {
                    iter_l1.push(snapshot.sstables.get(sst).unwrap().clone());
                }

                // 合成一个Iterator
                let iter = TwoMergeIterator::create(
                    MergeIterator::create(iter_l0),
                    SstConcatIterator::create_and_seek_to_first(iter_l1)?,
                )?;
                self.compact_iter_to_ssts(iter, _task.compact_to_bottom_level())
            }

            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _,
                lower_level_sst_ids,
                ..
            }) => match upper_level {
                // l_i和l_i+1合并
                Some(_) => {
                    let mut iter_upper_ssts = Vec::new();
                    for sst in upper_level_sst_ids.iter() {
                        iter_upper_ssts.push(snapshot.sstables.get(sst).unwrap().clone());
                    }

                    let mut iter_lower_ssts = Vec::new();
                    for sst in lower_level_sst_ids.iter() {
                        iter_lower_ssts.push(snapshot.sstables.get(sst).unwrap().clone());
                    }

                    // 两个SstConcatIterator
                    let iter = TwoMergeIterator::create(
                        SstConcatIterator::create_and_seek_to_first(iter_upper_ssts)?,
                        SstConcatIterator::create_and_seek_to_first(iter_lower_ssts)?,
                    )?;
                    self.compact_iter_to_ssts(iter, _task.compact_to_bottom_level())
                }

                None => {
                    // 合并l0和l1
                    let mut iter_upper_ssts = Vec::new();
                    // l0层建立iterator
                    for sst in upper_level_sst_ids.iter() {
                        iter_upper_ssts.push(Box::new(SsTableIterator::create_and_seek_to_first(
                            snapshot.sstables.get(sst).unwrap().clone(),
                        )?));
                    }

                    let mut iter_lower_ssts = Vec::new();
                    for sst in lower_level_sst_ids.iter() {
                        iter_lower_ssts.push(snapshot.sstables.get(sst).unwrap().clone());
                    }

                    // l0的MergeIterator和l1的SstConcatIterator
                    let iter = TwoMergeIterator::create(
                        MergeIterator::create(iter_upper_ssts),
                        SstConcatIterator::create_and_seek_to_first(iter_lower_ssts)?,
                    )?;
                    self.compact_iter_to_ssts(iter, _task.compact_to_bottom_level())
                }
            },

            CompactionTask::Tiered(TieredCompactionTask { tiers, .. }) => {
                println!("1111111111111111111111111111111111");
                let mut iters = Vec::new();
                for (_, ssts) in tiers.iter() {
                    let mut iters_sst = Vec::new();
                    for sst in ssts.iter() {
                        iters_sst.push(snapshot.sstables.get(sst).unwrap().clone());
                    }
                    iters.push(Box::new(SstConcatIterator::create_and_seek_to_first(
                        iters_sst,
                    )?));
                }

                let iter = MergeIterator::create(iters);
                self.compact_iter_to_ssts(iter, _task.compact_to_bottom_level())
            }

            _ => {
                unimplemented!()
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };

        let l0_sstables = snapshot.l0_sstables.clone();
        let l1_sstables = snapshot.levels[0].1.clone();
        let compaction_task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };

        let sstables = self.compact(&compaction_task)?;
        {
            let _state_lock = self.state_lock.lock();
            let mut state = self.state.read().as_ref().clone();

            for id in l0_sstables.iter().chain(l1_sstables.iter()) {
                state.sstables.remove(id);
            }

            let mut ids = Vec::new();
            for sstable in sstables {
                ids.push(sstable.sst_id());
                state.sstables.insert(sstable.sst_id(), sstable);
            }

            // level1变成当前compact完的sst
            state.levels[0].1 = ids;
            let mut l0_sstables_map = l0_sstables.iter().collect::<HashSet<_>>();
            // 把state.l0_sstables中进行了compaction的sst全部删掉，即l0_sstables中所有元素
            state.l0_sstables = state
                .l0_sstables
                .iter()
                .copied()
                .filter(|x| !l0_sstables_map.remove(x))
                .collect::<Vec<_>>();

            // 开写锁把新l0/l1状态写进state
            *self.state.write() = Arc::new(state);
        }

        // 删除这些被compact的SST文件
        for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(*sst))?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        println!("3333333333333333333333333333333333");
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        println!("5555555555555555555555555555555555");
        let task = self
            .compaction_controller
            .generate_compaction_task(&snapshot);
        println!("4444444444444444444444444444444444");
        let Some(task) = task else {
            return Ok(());
        };
        self.dump_structure();
        // compact后生成的sst
        let sstables = self.compact(&task)?;
        // 新加入的sst_id
        let output = sstables.iter().map(|sst| sst.sst_id()).collect::<Vec<_>>();
        let ssts_to_remove = {
            let _state_lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();
            // 里面把snapshot被compaction的两层清空，然后把compact后的id填入下层；返回的files_to_remove包含了被清空的两层的sst_id，用于把snapshot里存的sst指针和文件删除
            for sst in sstables {
                snapshot.sstables.insert(sst.sst_id(), sst);
            }
            let (mut snapshot, files_to_remove) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &output, false);
            let mut ssts_to_remove = Vec::new();
            for file in files_to_remove {
                let result = snapshot.sstables.remove(&file);
                ssts_to_remove.push(result.unwrap());
            }
            // 把compact后的sst插入对应位置
            let mut state = self.state.write();
            *state = Arc::new(snapshot);
            drop(state);
            ssts_to_remove
        };

        // 删除被删除的sst文件
        for sst in ssts_to_remove {
            std::fs::remove_file(self.path_of_sst(sst.sst_id()))?;
        }

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        if {
            let state = self.state.read();
            state.imm_memtables.len() >= self.options.num_memtable_limit
        } {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
