# Elbow

An async pipeline builder

## Features

* **Type-Safe Pipelines**: Compose chains of processors (`Pipe<I, O>`) that transform input types to output types.
* **Dynamic Composition**: Build pipelines at runtime using `PipelineBuilder` and the `PipelineExt` trait.
* **Filtering and Tapping**: Use `.filter()` to drop unwanted items and `.tee()` to inspect items without modifying the stream.
* **Batching and Chunking**: Group items into batches by size or time window with `.batch()` and `.chunk()`.
* **Concurrency**: Parallelize processing with `.concurrent()` to distribute work across multiple workers.
* **Backpressure-Free Channels**: Leverage `async_channel` under the hood for smooth, unbounded message passing.
* **Utilities**: Join batched streams back into individual items with `.join()`.

## Installation

Add Elbow to your `Cargo.toml`:

```toml
[dependencies]
elbow = "0.1"
```

Ensure you have the Tokio runtime enabled:

```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
```

## Quick Start

```rust
use elbow::{PipelineBuilder, Pipe};
use async_channel::{unbounded, Receiver, Sender};
use std::any::Any;

struct StringToInt;
impl Pipe<String, i32> for StringToInt {
    async fn process(&self, input: String) -> Option<i32> {
        input.parse().ok()
    }
}

struct Double;
impl Pipe<i32, i32> for Double {
    async fn process(&self, num: i32) -> Option<i32> {
        Some(num * 2)
    }
}

#[tokio::main]
async fn main() {
    // Build a pipeline: parse -> double
    let (input, output) = PipelineBuilder::first(StringToInt)
        .then(Double)
        .build();

    input.handle(vec!["1", "2", "a", "4"]);

    // Receive and print outputs
    while let Some(val) = recv.next().await {
        println!("Output: {}", val);
    }
}
```

## API Overview

### Core Traits

* `trait Pipe<I, O>`: Defines an asynchronous processor transforming `I` into `Option<O>`.
* `struct Context`: Manages input/output channels for a running pipe.
* `trait DynPipe`: Internal trait for type-erased pipes.

### Pipeline Builder

Use `PipelineBuilder::first(...)` to start, then chain:

* `.then(pipe)`: Append a new `Pipe`.
* `.map(fn)`: Map function into a pipe.
* `.filter(fn)`: Drop items not satisfying the predicate.
* `.tee(fn)`: Inspect items for logging or side-effects.
* `.chunk(duration, size)`: Emit `Vec<I>` batches by time or count.
* `.join()`: Flatten batched streams back to individual items.
* `.build()`: Returns `(Receiver<Output>, Sender<Input>)`.

### Extension Methods

Pipes that implement `Clone` automatically gain:

* `.concurrent(n)`: Run `n` clones in parallel.
* `.batch(size, window)`: Buffer and send batches downstream.

## Advanced Usage

Combine batching with concurrency for high-throughput:

```rust
use std::time::Duration;

let (recv, send) = PipelineBuilder::first(ProcessItem)
    .then(
        ExpensivePipe
            .batch(20, Duration::from_millis(50))
            .concurrent(4),
    )
    .build();
```

## Contributing

Contributions are welcome! Please open issues or submit pull requests on [GitHub](https://github.com/yourusername/elbow).

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes
4. Push to your fork and submit a PR

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
