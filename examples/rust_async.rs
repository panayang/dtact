use dtact::{dtact_await, dtact_init, spawn, task, yield_now};

#[task(
    priority = "Normal",
    kind = "Compute",
    stack = "256K",
    capacity = "1024"
)]
async fn worker(id: u32) {
    println!("[Fiber {}] Starting async work...", id);

    for i in 0..3 {
        println!("[Fiber {}] Progress step {}", id, i);
        yield_now().await;
    }

    println!("[Fiber {}] Task Finished.", id);
}

#[dtact_init(workers = 4, stack = "256K", capacity = "1024")]
fn main() {
    println!("--- Dtact Rust Macro Example ---");

    let mut handles = vec![];
    for i in 0..5 {
        println!("[Master] Launching Fiber {}", i);
        // spawn takes a future and returns a handle
        handles.push(spawn(worker(i)));
    }

    for (i, handle) in handles.into_iter().enumerate() {
        println!("[Master] Waiting for Fiber {} to complete...", i);
        dtact_await(handle);
        println!("[Master] Fiber {} has been joined.", i);
    }

    println!("[Master] All sub-tasks completed. Exiting cleanly.");
}
