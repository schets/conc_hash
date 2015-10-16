#ifndef ATOMICS_H
#define ATOMICS_H

//define our own atomics, don't rely on C11
#define mem_relaxed __ATOMIC_RELAXED
#define mem_acquire __ATOMIC_ACQUIRE
#define mem_release __ATOMIC_RELEASE
#define mem_acq_rel __ATOMIC_ACQ_REL
#define mem_seq_cst __ATOMIC_SEQ_CST

//we don't support dec alpha
#define consume_barrier __atomic_signal_fence(mem_acquire)
#define compiler_barrier __atomic_signal_fence(mem_seq_cst)
#define atomic_barrier(o) __atomic_thread_fence(o)

#define atomic_load(n, o) __atomic_load_n(&(n), o)
#define atomic_store(n, v, o) __atomic_store_n(&(n), (v), o)

#define atomic_fetch_add(n, v, o) __atomic_fetch_add(&(n), v, o)
#define atomic_fetch_sub(n, v, o) __atomic_fetch_sub(&(n), v, o)

//ewwwwww
#define _GET_MACRO(_1,_2,_3,_4,NAME,...) NAME

#define _atomic_cas4(e, n, o, lo)
#define _atomic_cas3(e, n, o) _atomic_cas4(&(e), (n), o, mem_relaxed)

#define atomic_cas(...) _GET_MACRO(__VA_ARGS__, _atomic_cas4, _atomic_cas3)(__VA_ARGS__)


#endif
