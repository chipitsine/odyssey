#ifndef MM_CALL_H
#define MM_CALL_H

/*
 * machinarium.
 *
 * cooperative multitasking engine.
*/

typedef struct mm_call mm_call_t;

typedef void (*mm_cancel_t)(void*, void *arg);

struct mm_call
{
	int             active;
	mm_coroutine_t *coroutine;
	mm_timer_t      timer;
	mm_cancel_t     cancel_function;
	void           *arg;
	int             timedout;
	int             status;
};

void mm_call(mm_call_t*, int);
void mm_call_fast(mm_call_t*, void (*)(void*), void*);

static inline int
mm_call_is_active(mm_call_t *call)
{
	return call->active;
}

static inline int
mm_call_is_aborted(mm_call_t *call)
{
	return call->active && call->status != 0;
}

static inline void
mm_call_cancel(mm_call_t *call, void *object)
{
	if (! call->active)
		return;
	call->cancel_function(object, call->arg);
}

#endif /* MM_CALL_H */
