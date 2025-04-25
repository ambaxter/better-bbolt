# Mut Transactions
* Where do we store each bucket's keys/values?
```rust
pub enum ValueDelta {
  UValue(SharedData),
  UBucket(SharedData),
  Delete,
}

pub struct CoreMutBucket<'a, T> {
  pub(crate) tx: &'a T,
  pub(crate) delta_map: MutexGuard<'a, BTreeMap<SharedData, ValueDelta>>
}

// Then Cursor/Bucket return types in an enum of &'tx [u8] and the regular return
// So many combo types :D
```
* Where do we store each bucket's information in a rw transaction?

```rust
use std::collections::{BTreeMap};
use std::sync::Arc;

pub struct BucketMap {
  map: BTreeMap<Vec<SharedData>, Arc<Mutex<BTreeMap<SharedData, ValueDelta>>>>
}
```
* How do we reference each bucket inside a rw transaction?

# Mut Commitment
* For a single bucket how do we place data amongst siblings?
* How do we cascade up buckets to the root?
* How do we 