/*
 * Odyssey.
 *
 * Scalable PostgreSQL connection pooler.
 */

#include <odyssey.h>

#include <machinarium/machinarium.h>

#include <thread_global.h>
#include <global.h>
#include <instance.h>
#include <od_memory.h>

od_retcode_t od_thread_global_init(od_thread_global **gl)
{
	if (gl == NULL) {
		return NOT_OK_RESPONSE;
	}

	od_thread_global *new_gl = od_malloc(sizeof(od_thread_global));
	if (new_gl == NULL) {
		return NOT_OK_RESPONSE;
	}

	od_instance_t *instance = od_global_get_instance();
	if (instance == NULL) {
		od_free(new_gl);
		return NOT_OK_RESPONSE;
	}

	if (od_conn_eject_info_init(&new_gl->info,
				    &instance->config.conn_drop_options) != 0) {
		od_free(new_gl);
		return NOT_OK_RESPONSE;
	}

	*gl = new_gl;
	return OK_RESPONSE;
}

od_thread_global **od_thread_global_get(void)
{
	return (od_thread_global **)machine_thread_private();
}

od_retcode_t od_thread_global_free(od_thread_global *gl)
{
	if (gl == NULL) {
		return NOT_OK_RESPONSE;
	}
	od_retcode_t rc = od_conn_eject_info_free(gl->info);

	if (rc != OK_RESPONSE) {
		return rc;
	}

	od_free(gl);

	return OK_RESPONSE;
}
