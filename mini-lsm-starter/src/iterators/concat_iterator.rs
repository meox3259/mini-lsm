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

use std::sync::Arc;

use anyhow::Result;
use clap::builder::NonEmptyStringValueParser;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables: sstables,
            });
        }

        let mut iter = Self {
            current: Some(SsTableIterator::create_and_seek_to_first(
                sstables[0].clone(),
            )?),
            next_sst_idx: 1,
            sstables: sstables,
        };

        iter.move_until_valid()?;

        Ok(iter)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let index = sstables
            .partition_point(|sst| sst.first_key().as_key_slice() <= key)
            .saturating_sub(1);
        if index >= sstables.len() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables: sstables,
            });
        }
        let mut iter = Self {
            current: Some(SsTableIterator::create_and_seek_to_key(
                sstables[index].clone(),
                key,
            )?),
            next_sst_idx: index + 1,
            sstables: sstables,
        };
        iter.move_until_valid()?;
        Ok(iter)
    }

    fn move_until_valid(&mut self) -> Result<()> {
        loop {
            if let Some(iter) = self.current.as_mut() {
                if iter.is_valid() {
                    break;
                }
                if self.next_sst_idx >= self.sstables.len() {
                    self.current = None;
                } else {
                    self.current = Some(SsTableIterator::create_and_seek_to_first(
                        self.sstables[self.next_sst_idx].clone(),
                    )?);
                    self.next_sst_idx += 1;
                }
            } else {
                break;
            }
        }
        Ok(())
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        if let Some(iter) = &self.current {
            return iter.is_valid();
        } else {
            return false;
        }
    }

    fn next(&mut self) -> Result<()> {
        self.current.as_mut().unwrap().next()?;
        self.move_until_valid()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
