# Reliable File Transfer Protocol over Simulated Network

A C++ client-server application that demonstrates reliable file transfer over a simulated network link. This project utilizes a custom transport layer API to handle connection setups, data transmission, and multiplexing, bypassing standard TCP sockets to manually manage network constraints like bandwidth limits and propagation delays.

## 🚀 Key Features

*   **Custom Transport API:** Uses a simulated network layer (`send_data`, `recv_data`, `wait4connect`) to handle reliable data delivery.
*   **Multi-Client Multiplexing:** The server is capable of accepting and processing concurrent file transfers from multiple clients simultaneously, writing the output to separate, dynamically generated files.
*   **Network Simulation:** The client initializes with specific network parameters (e.g., 8 Mb/s bandwidth, 2ms delay) to simulate real-world physical link limitations.
*   **Performance Benchmarking:** Built-in macros (`TICK` and `TOCK`) utilizing `CLOCK_MONOTONIC_RAW` to accurately measure throughput and transfer completion times.
*   **Chunk-Based Asynchronous I/O:** Files are read, transmitted, and written in discrete chunks (512 - 1024 bytes) to prevent memory exhaustion and respect simulated window sizes.

## 🧠 Architecture

1.  **Handshake:** The client establishes a connection using `setup_connection`. The server accepts via `wait4connect`.
2.  **Metadata Transfer:** The client reads the target file, calculates its exact byte size using `lseek`, and transmits this 32-bit integer to the server.
3.  **Data Streaming:** The client loops through the file, sending chunks. It implements basic flow control by sleeping (`usleep`) if the sender window/buffer is full.
4.  **Multiplexed Receiving:** In multi-client mode, the server polls incoming data across multiple connection IDs, draining the network buffers and writing to disk until all exact file sizes are satisfied.

## 🛠️ Tech Stack

*   **Language:** C++
*   **Libraries:** POSIX Sockets, `<ctime>` (Monotonic Clocks), standard POSIX I/O (`open`, `read`, `write`, `lseek`).

## ⚙️ How to Build and Run

### Prerequisites
*   A Linux environment with `g++` and `make`.
*   The underlying simulated network library (`lib.h` and compiled object files).

### Execution

**Start the Server:**
Run the server. Pass an integer argument to specify the number of expected concurrent clients (defaults to 1 if no argument is provided).
```bash
./server         # Single client mode
./server 3       # Multi-client mode (waits for 3 connections)
