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

use crate::key::{KeySlice, KeyVec};

use super::Block;

use bytes::BufMut;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

fn compute_overlap(first_key: &[u8], key: &[u8]) -> usize {
    let mut i = 0;
    while i < first_key.len() && i < key.len() {
        if first_key[i] == key[i] {
            i += 1;
        } else {
            break;
        }
    }
    i
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size: block_size,
            first_key: KeyVec::new(),
        }
    }

    pub fn estimated_size(&self) -> usize {
        size_of::<u16>() + self.data.len() + size_of::<u16>() * self.offsets.len()
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if self.estimated_size() + key.raw_len() + value.len() + size_of::<u16>() * 3
            > self.block_size
            && !self.is_empty()
        {
            return false;
        }
        self.offsets.push(self.data.len() as u16);
        let overlap = compute_overlap(self.first_key.key_ref(), key.key_ref());
        self.data.put_u16(overlap as u16);
        self.data.put_u16((key.key_len() - overlap) as u16);
        let _key = key.key_ref();
        self.data.put(&_key[overlap..]);
        self.data.put_u64(key.ts());
        self.data.put_u16(value.len() as u16);
        self.data.put(value);

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty() && self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
