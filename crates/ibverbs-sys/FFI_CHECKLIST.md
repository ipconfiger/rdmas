# FFI Binding Completeness Checklist

> Generated: 2026-08-02
> Source: `crates/ibverbs-sys/` (bindgen 0.22.1, rdma-core 61.0, libibverbs.so.1.15.61.0)
> All 2325 lines of generated bindings verified symbol-by-symbol against `nm -D` output.

## Verdict: **Nothing Missing** ✅

All functions needed for Waves 8–11 are already present in the generated bindings.

## Symbol Audit

### T8-B — QP Recovery

| Symbol | Status | Notes |
|--------|--------|-------|
| `ibv_query_qp` | ✅ PRESENT | bindings.rs:2250, exported `@@IBVERBS_1.1` |
| `ibv_modify_qp` | ✅ PRESENT | bindings.rs:2236, already used in `src/rdma/qp.rs` |
| `ibv_qp_attr` struct | ✅ PRESENT | bindings.rs:1184, full struct with all state fields |
| `ibv_qp_attr_mask` enum | ✅ PRESENT | All `IBV_QP_*` masks available |
| `ibv_qp_init_attr` struct | ✅ PRESENT | Required 4th arg of `ibv_query_qp` |

### T8-C — CQ Event Handling

| Symbol | Status | Notes |
|--------|--------|-------|
| `ibv_create_comp_channel` | ✅ PRESENT | bindings.rs:2177, `@@IBVERBS_1.0` |
| `ibv_destroy_comp_channel` | ✅ PRESENT | bindings.rs:2180 |
| `ibv_get_cq_event` | ✅ PRESENT | bindings.rs:2198, `@@IBVERBS_1.1` |
| `ibv_ack_cq_events` | ✅ PRESENT | bindings.rs:2205, `@@IBVERBS_1.1` |
| `ibv_comp_channel` struct | ✅ PRESENT | `fd` field directly accessible for epoll |
| `ibv_req_notify_cq_wr` | ✅ PRESENT | C wrapper in `lib.rs` + `wrapper_fns.c` |

### T11-B — Async Events

| Symbol | Status | Notes |
|--------|--------|-------|
| `ibv_get_async_event` | ✅ PRESENT | bindings.rs:2065, `@@IBVERBS_1.1` |
| `ibv_ack_async_event` | ✅ PRESENT | bindings.rs:2071 |
| `ibv_async_event` struct | ✅ PRESENT | Complete struct with union element |
| `ibv_event_type` enum | ✅ PRESENT | All `IBV_EVENT_*` values |

## Usage Notes

1. **`ibv_req_notify_cq`**: This is a static inline function in verbs.h. Use the existing `ibv_req_notify_cq_wr` C wrapper from `ibverbs-sys/src/wrapper_fns.c`.
2. **`ibv_get_cq_event` / `ibv_get_async_event`**: These block on fd. For non-blocking usage, set `O_NONBLOCK` on `ibv_comp_channel.fd` and use `epoll`.
3. **`ibv_query_qp`**: Requires `attr_mask` parameter — use `ibv_qp_attr_mask::IBV_QP_STATE` etc.
