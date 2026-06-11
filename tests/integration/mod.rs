//! Integration tests for peat-sapient against the Dstl Apex SAPIENT middleware.
//!
//! Run with:
//! ```sh
//! cargo test --features integration-tests,peat -- --test-output immediate
//! ```
//!
//! Tests automatically skip when `apex.py` is not on PATH. To run them:
//! 1. Install Apex: https://github.com/dstl/Apex-SAPIENT-Middleware
//! 2. Ensure `apex.py` is executable and on your PATH.
//!
//! Two sets of tests:
//! - `inbound_flow`  — peat-sapient receives SAPIENT messages from Apex and routes them
//! - `outbound_flow` — peat-sapient sends Tasks; two loopback tests do not require Apex

mod apex_harness;
mod inbound_flow;
mod outbound_flow;
