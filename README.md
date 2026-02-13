# bitfinex-feeder

A high-performance Rust-based market data feeder that streams real-time order book and trade data from Bitfinex into a shared memory (SHM) ring buffer.

This tool is designed for High-Frequency Trading (HFT) applications where low-latency communication between the data ingestor and the execution strategy is critical.

## Features

* **Real-time Streaming**: Connects to the Bitfinex WebSocket API (`v2`) to receive live `tBTCUSD` updates.
* **Shared Memory IPC**: Utilizes `/dev/shm/hft_ring` for ultra-low latency inter-process communication using memory-mapped files.
* **Lock-Free Ring Buffer**: Implements a volatile-read/write ring buffer with memory fences to ensure thread-safe data transfer without the overhead of traditional locks.
* **Data Normalization**: Packs market events into a compact, C-compatible struct (`#[repr(C)]`) for easy consumption by other languages.

## System Requirements

* **Operating System**: Linux (required for `/dev/shm` shared memory support).
* **Rust**: Edition 2024.

## Data Structure

The feeder writes `Data` packets into the ring buffer with the following memory layout:

### The Data Packet
Prices are scaled by 100 (to preserve two decimal places in an integer) and sizes are scaled by 1,000,000.

| Field | Type | Description |
| :--- | :--- | :--- |
| `id` | `u64` | The Order or Trade ID. |
| `size` | `u64` | Absolute volume (scaled by 1,000,000). |
| `price` | `i64` | Price (scaled by 100). |
| `side` | `i8` | `0` for Bid/Buy, `1` for Ask/Sell. |
| `action` | `i8` | `0` for Update/Add, `1` for Cancel/Delete (Price = 0.0). |
| `status` | `i8` | `0` for Book Update, `1` for Trade Execution. |

### Shared Memory Layout
The shared memory file at `/dev/shm/hft_ring` follows this structure:

1.  **Write Index (`u64`)**: The current position of the producer.
2.  **Padding**: 56 bytes to prevent cache line contention.
3.  **Read Index (`u64`)**: The current position of the consumer.
4.  **Padding**: 56 bytes.
5.  **Buffer**: A fixed-size array of 16,384 `Data` slots.

## Dependencies

Major dependencies used in this project include:
* `tokio`: Async runtime for WebSocket management.
* `tokio-tungstenite`: WebSocket protocol implementation.
* `memmap2`: Memory-mapped file IO.
* `serde_json`: Parsing Bitfinex API responses.

## Usage

1.  **Build the project**:
    ```bash
    cargo build --release
    ```
2.  **Run the feeder**:
    ```bash
    cargo run --release
    ```
    Upon starting, the feeder will create or open `/dev/shm/hft_ring`, subscribe to Bitfinex book and trade channels, and begin populating the ring buffer.

## Implementation Notes
* **Volatility**: The ring buffer uses `ptr::read_volatile` and `ptr::write_volatile` to ensure memory operations are not skipped by the compiler.
* **Memory Fences**: A `Release` fence is used before updating the write index to ensure data visibility to consumers.
