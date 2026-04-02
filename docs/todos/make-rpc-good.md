I want to make the Fitz RPC domain genuinely excellent for real production use, not just functionally correct or benchmark-fast in ideal cases.

Treat this as a serious systems design and implementation review for an RPC subsystem inside a distributed runtime. I personally expect to rely on this domain heavily, so I care most about:

- correctness
- predictability
- backpressure behavior
- fairness
- timeout behavior
- streaming response correctness
- cleanup correctness
- operational trust

Assume the Fitz RPC model includes concepts like:

- request / response
- worker subscription to RPC routes
- dispatch to workers
- pending request tracking
- reply inbox / correlation IDs
- timeouts
- backpressure when queues are full
- streaming multi-chunk responses
- stream end markers
- worker disconnects
- requester disconnects
- lease or ownership tracking while requests are in flight
- cleanup on terminal response or disconnect

I want a TDD-driven implementation and audit plan.

Your task:

1. Define the correct RPC semantics

Spell out the exact semantics Fitz RPC should have.

Answer:

- Is Fitz RPC unary-only, streaming, or both?
- What exactly makes a request complete?
- What exactly makes a stream complete?
- What guarantees exist around request ordering?
- What guarantees exist around worker selection and fairness?
- What should happen if no worker exists?
- What should happen if workers subscribe after requests are already pending?
- What should happen if a worker disconnects during a request?
- What should happen if the requester disconnects during a request?
- What should happen if a response arrives after timeout?
- What should happen if a duplicate chunk arrives?
- What should happen if chunks arrive out of order?
- What should happen if stream_end=true arrives before prior chunks?
- What should happen if cleanup occurs before final response delivery?

Be precise. I do not want vague language.

2. Define core invariants

List the non-negotiable invariants, such as:

- A correlation ID must map to at most one active pending request
- A response must never be delivered to the wrong requester
- A completed request must be removed from pending state exactly once
- A timed-out request must not later reappear as active
- A request must not be simultaneously owned by multiple workers unless explicitly designed that way
- A streaming response must preserve logical chunk ordering at delivery
- Duplicate response chunks must not produce duplicate user-visible output
- Cleanup must not leak pending requests or worker ownership state
- Backpressure must fail predictably, not silently drop work

For each invariant:

- explain why it matters
- explain how a bad implementation would violate it

3. Identify the dangerous RPC bug classes

I want an explicit list of bug classes, including:

- request leaks in pending table
- response delivered to wrong caller
- worker lease not released
- timeout cleanup races
- late response after timeout causing corruption
- duplicate chunk delivery
- out-of-order chunk reassembly bugs
- terminal chunk cleanup race
- requester disconnect race
- worker unsubscribe race
- correlation ID collision or reuse bugs
- queue-full behavior causing inconsistent state
- pending-capacity enforcement bugs
- starvation or unfair worker distribution
- reply inbox cleanup bugs
- partial stream cleanup bugs

For each bug class:

- explain the failure mode
- explain likely cause
- explain how to test for it

4. Define the correct internal state model

Propose the cleanest internal state model for the RPC actor and related structures.

I want explicit state definitions for:

- registered workers by route
- pending requests
- queued requests when no worker is available
- request ownership / lease tracking if applicable
- reply inbox / chunk buffer state
- timeout tracking
- worker metrics
- requester cleanup tracking
- route-level dispatch state

Be explicit about:

- what must be persisted
- what can remain in memory
- what can be reconstructed
- what must be cleaned up synchronously vs asynchronously

5. Define dispatch semantics

Review the dispatch model and define expected behavior for:

- round robin worker distribution
- single worker saturation
- many workers on same route
- many routes sharing workers
- no workers available
- workers registering after requests queue up
- route mismatch / family mismatch

I want a precise fairness and dispatch model.

6. Define timeout behavior

Spell out exact timeout semantics.

Answer:

- When does timeout start?
- Is timeout based on dispatch time or enqueue time?
- What happens if request sits pending without worker?
- What happens if a worker sends a response after timeout?
- What cleanup must happen on timeout?
- Must timeout errors include correlation IDs?
- How do timeouts interact with streaming responses?

Test:

- single timed-out request
- many timed-out requests
- timeout sweep under load
- timeout with requester disconnect
- timeout with worker disconnect

7. Define streaming response behavior

This is critical.

Spell out exact semantics for:

- chunk ordering
- chunk buffering
- duplicate chunk handling
- gap detection
- final chunk handling
- out-of-order final chunk
- buffered final chunk flush
- stream cleanup

I want explicit discussion of:

- sequence numbers
- correlation IDs
- stream_end semantics
- buffer overflow handling
- what makes a stream terminal
- whether partial delivery is allowed before gap fill

Be very specific.

8. Define disconnect and cleanup behavior

Review every cleanup path.

Answer:

- What happens when requester disconnects before response?
- What happens when requester disconnects mid-stream?
- What happens when worker disconnects while owning requests?
- What happens when worker unsubscribes from route?
- What happens when cleanup message arrives explicitly?
- What happens when terminal response has stream_end=true?
- What happens when terminal response is duplicated after cleanup?

I want no ambiguity.

9. Define backpressure semantics

I want Fitz RPC to fail predictably under pressure.

Answer:

- What happens when pending capacity is full?
- What happens when worker queue is full?
- What happens when reply inbox buffer overflows?
- What error should be returned?
- What state must remain clean after rejection?
- What metrics/logging must fire?

Make sure backpressure is explicit, deterministic, and easy to observe.

10. Define crash and restart expectations

Be explicit:

- Is RPC state in-memory only?
- Are pending requests recoverable after restart?
- What should happen to in-flight requests on restart?
- Should callers receive errors, retry, or lose requests?
- How should worker state recover?
- How should reply inbox state behave on restart?

If RPC is intentionally ephemeral, say so clearly and define the contract honestly.

11. Define observability requirements

I want RPC to be operationally trustworthy.

Define required metrics and admin visibility for:

- requests/sec by route
- responses/sec by route
- pending requests by route
- queued requests by route
- timeout count
- backpressure count
- worker count by route
- dispatch latency
- end-to-end latency
- response chunk count
- active streams
- cleanup count
- worker disconnect errors
- late response drops
- duplicate chunk drops
- out-of-order chunk buffering

Also define what the admin dashboard should show for RPC.

12. Define benchmark plan

I want a benchmark plan that proves RPC is good, not just one happy-path microbenchmark.

Include benchmarks for:

- direct request/response
- encoded request/response
- ws request/response
- tcp request/response
- dispatch-only throughput
- full roundtrip throughput
- worker pool scaling
- multi-route dispatch scaling
- pending request tracking overhead
- timeout sweep cost
- worker subscribe/unsubscribe cost
- streaming chunk reassembly
- out-of-order buffering
- requester disconnect cleanup
- worker disconnect cleanup
- concurrent request load

For each benchmark say:

- what it proves
- what a bad result would indicate
- what optimization category it targets

13. Define the test plan

I want a rigorous TDD checklist.

Break tests into:

- unit tests
- integration tests
- streaming correctness tests
- timeout tests
- cleanup/disconnect tests
- concurrency tests
- fairness tests
- scale tests
- failure tests
- invariant/property tests if useful

Prioritize the highest-value tests first.

14. Define likely implementation risks

Based on a subsystem like this, tell me the most likely architectural mistakes, such as:

- pending table cleanup bugs
- worker ownership leaks
- timeout sweep races
- non-idempotent terminal cleanup
- weak correlation handling
- incorrect stream end semantics
- buffer growth bugs
- unfair worker dispatch
- conflating transport ordering with logical ordering
- over-optimizing dispatch while under-testing cleanup paths

Be opinionated and specific.

15. Define production-readiness criteria

I want explicit criteria for when Fitz RPC can be considered production-trustworthy.

Score and define requirements for:

- correctness
- timeout behavior
- cleanup correctness
- fairness
- observability
- latency predictability
- backpressure behavior
- streaming correctness
- disconnect handling
- operational debuggability

16. Produce the final output in this structure

I want the answer structured as:

A. RPC semantic contract
B. Non-negotiable invariants
C. Dangerous bug classes
D. Required internal state model
E. Dispatch and fairness model
F. Timeout model
G. Streaming response model
H. Disconnect and cleanup model
I. Backpressure model
J. Restart / failure model
K. Observability requirements
L. Benchmark plan
M. Test plan
N. Top implementation risks
O. Recommended next implementation priorities

Important constraints:

- Prefer boring, correct, predictable behavior over cleverness
- Prioritize cleanup correctness and timeout correctness over raw peak throughput
- Be explicit about race conditions and terminal states
- Assume hidden cleanup bugs exist until proven otherwise
- Be critical and specific
