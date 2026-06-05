use cute_sqlite_kv::KVStore;
use std::sync::{Arc, Barrier};

// Regression test for the WAL cold-open race: when many processes/threads
// open the same brand-new database file at once, the PRAGMA
// journal_mode=WAL switch is not covered by busy_timeout and used to
// return "database is locked" for a fraction of opens. enable_wal retries,
// so every open must now succeed.
//
// This is a probabilistic guard (the race is timing-dependent), but it
// never fails spuriously on correct code -- post-fix it passed thousands
// of opens with zero failures.
#[test]
fn concurrent_cold_open_never_locks() {
    let dir = tempfile::tempdir().unwrap();
    let rounds = 50;
    let threads = 16;

    for r in 0..rounds {
        let path = dir.path().join(format!("db_{r}.sqlite"));
        let barrier = Arc::new(Barrier::new(threads));
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let p = path.clone();
                let b = barrier.clone();
                std::thread::spawn(move || {
                    b.wait();
                    // Open, and exercise a write so each opener really uses
                    // the store, not just creates it.
                    let store = KVStore::new_from_file(&p)
                        .map_err(|e| e.to_string())
                        .expect("concurrent cold open should not fail");
                    store.insert("k", "v");
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
    }
}
