/* Thin wrappers around static inline functions from verbs.h
 * that bindgen cannot see. These are compiled into the crate
 * so the Rust FFI code can call them. */

#include <infiniband/verbs.h>
#include <stdint.h>

int ibv_post_send_wr(struct ibv_qp *qp, struct ibv_send_wr *wr,
                     struct ibv_send_wr **bad_wr) {
    return ibv_post_send(qp, wr, bad_wr);
}

int ibv_post_recv_wr(struct ibv_qp *qp, struct ibv_recv_wr *wr,
                     struct ibv_recv_wr **bad_wr) {
    return ibv_post_recv(qp, wr, bad_wr);
}

int ibv_poll_cq_wr(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc) {
    return ibv_poll_cq(cq, num_entries, wc);
}

int ibv_req_notify_cq_wr(struct ibv_cq *cq, int solicited_only) {
    return ibv_req_notify_cq(cq, solicited_only);
}

/* Wrapper around ibv_query_port that takes ibv_port_attr* directly,
 * avoiding the _compat type issue. */
int ibv_query_port_attr(struct ibv_context *context, uint8_t port_num,
                        struct ibv_port_attr *port_attr) {
    return ___ibv_query_port(context, port_num, port_attr);
}

/* Accessors for opaque ibv_mr fields (ibv_mr is opaque in bindgen output). */
uint32_t ibv_mr_lkey(struct ibv_mr *mr) { return mr->lkey; }
uint32_t ibv_mr_rkey(struct ibv_mr *mr) { return mr->rkey; }
void *ibv_mr_addr(struct ibv_mr *mr) { return mr->addr; }
size_t ibv_mr_length(struct ibv_mr *mr) { return mr->length; }

/* Accessor for opaque ibv_qp.qp_num field. */
uint32_t ibv_qp_get_qp_num(struct ibv_qp *qp) { return qp->qp_num; }
