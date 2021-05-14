

/*
 * Odyssey.
 *
 * Scalable PostgreSQL connection pooler.
 */

#include <machinarium.h>
#include <odyssey.h>

const char *argp_program_version;
const char *argp_program_bug_address = "<x4mmm@yandex-team.ru>";


od_retcode_t od_apply_validate_cli_args(od_logger_t *logger, od_config_t *conf,
					od_arguments_t *args)
{
	if (conf->daemonize && !args->console) {
		od_dbg_printf_on_dvl_lvl(
			1,
			"daemonize config opt is %d and console flag is %d, so daemonizing process\n",
			conf->daemonize, args->console);
		conf->daemonize |= args->console;
	} else {
		conf->daemonize = 0;
	}

	if (args->silent && args->verbose) {
		od_log(logger, "startup", NULL, NULL,
		       "silent and verbose option both specified");
		return NOT_OK_RESPONSE;
	}

	if (args->silent) {
		conf->log_debug = 0;
		conf->log_session = 0;
		conf->log_query = 0;
	}
	if (args->verbose) {
		conf->log_debug = 1;
		conf->log_session = 1;
		conf->log_query = 1;
	}

	return OK_RESPONSE;
}
