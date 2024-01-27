use std::collections::HashSet;
use std::collections::HashMap;
use yellowstone_grpc_proto::prelude::CommitmentLevel;
use yellowstone_grpc_proto::prelude::SubscribeUpdateSlot;

#[derive(Clone, Debug)]
pub struct SlotData {
    pub slot: u64,
    pub parent: Option<u64>,
    pub children: Vec<u64>,
    pub status: CommitmentLevel,
    pub chain: u64, // the top slot that this is in a chain with. uncles will have values < tip
}

impl SlotData {
    pub fn from_update(slot_update: SubscribeUpdateSlot) -> Self {
        SlotData {
            slot: slot_update.slot,
            parent: slot_update.parent,
            children: vec![],
            status: CommitmentLevel::try_from(slot_update.status).unwrap(),
            chain: 0,
        }
    }

    pub fn add_child(&mut self, child: u64) {
        if !self.children.contains(&child) {
            self.children.push(child)
        }
    }
}

/// Track slots and forks
pub struct ChainData {
    /// only slots >= newest_finalized_slot are retained
    pub slots: HashMap<u64, SlotData>,
    pub forks_by_recent_slot: HashMap<u64, Vec<u64>>,
    pub newest_finalized_slot: u64,
    pub newest_processed_slot: u64,
    pub best_chain_slot: u64,
}

impl ChainData {
    pub fn new() -> Self {
        Self {
            slots: HashMap::new(),
            forks_by_recent_slot: HashMap::new(),
            newest_finalized_slot: 0,
            newest_processed_slot: 0,
            best_chain_slot: 0,
        }
    }
}

impl Default for ChainData {
    fn default() -> Self {
        Self::new()
    }
}

impl ChainData {
    /// Updates the ChainData with the provided slot
    pub fn update_slot(&mut self, new_slot: SlotData) {
        let new_processed_head = new_slot.slot > self.newest_processed_slot;
        if new_processed_head {
            self.newest_processed_slot = new_slot.slot;
        }

        let new_finalized_head = new_slot.slot > self.newest_finalized_slot
            && new_slot.status == CommitmentLevel::Finalized;
        if new_finalized_head {
            self.newest_finalized_slot = new_slot.slot;
        }

        // Use the highest slot that has a known parent as best chain
        // (sometimes slots OptimisticallyConfirm before we even know the parent!)
        let new_best_chain = new_slot.parent.is_some() && new_slot.slot > self.best_chain_slot;
        if new_best_chain {
            self.best_chain_slot = new_slot.slot;
        }

        let mut parent_update = false;

        use std::collections::hash_map::Entry;
        match self.slots.entry(new_slot.slot) {
            Entry::Vacant(v) => {
                v.insert(new_slot);
            }
            Entry::Occupied(o) => {
                let v = o.into_mut();
                parent_update = v.parent != new_slot.parent && new_slot.parent.is_some();
                v.parent = v.parent.or(new_slot.parent);
                // Never decrease the slot status
                if v.status == CommitmentLevel::Processed {
                    v.status = new_slot.status;
                } else if v.status == CommitmentLevel::Confirmed
                    && new_slot.status == CommitmentLevel::Finalized
                {
                    v.status = new_slot.status;
                }
            }
        };

        if new_best_chain || parent_update {
            let slots_to_visit: HashSet<u64> = self.slots.keys().cloned().collect();

            // re-link the chains
            // update the "chain" field down to the first finalized slot
            let slot = self.best_chain_slot;
            let remaining_slots = self.update_chain(slot, slots_to_visit);

            for remaining_slot in remaining_slots {
                // TODO: do less work
                self.update_chain(remaining_slot, HashSet::new());
            }
        }

        if new_finalized_head {
            // drop anything older than the most recent finalized slot
            self.slots.retain(|s, _| *s >= self.newest_finalized_slot);
        }

        // self.iter_chain(self.newest_finalized_slot);
    }

    /// Starting from the newest slot, iterate through the chain and update children
    /// Returns a vector of any unvisited slots
    fn update_chain(&mut self, mut slot: u64, mut slots_to_visit: HashSet<u64>) -> Vec<u64> {
        loop {
            let maybe_parent = if let Some(data) = self.slots.get_mut(&slot) {
                data.chain = self.best_chain_slot;
                slots_to_visit.remove(&slot);
                data.parent
            } else {
                None
            };

            if let Some(parent) = maybe_parent {
                self.slots.entry(parent).and_modify(|p| p.add_child(slot));
                slot = parent;
                continue;
            }
            break;
        }

        slots_to_visit.into_iter().collect()
    }
}