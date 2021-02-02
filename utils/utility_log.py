import logging
from utils.utility_commons import PATH, get_job_name

# global log variable
__default_logger = 'scrapy'
__log_map = dict()
__log_file_path = PATH['LOG_DIR'] + r'\{}.log'
__handlers = {}
# Logging config
LOG_CONFIG = {
    'version': 1,  # required
    'disable_existing_loggers': True,  # this config overrides all other loggers
    'formatters': {
        'brief': {
            'format': '%(levelname)s: %(message)s'
        },
        'precise': {
            'format': '%(asctime)s %(levelname)s - %(filename)s[line:%(lineno)s]: %(message)s'
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'precise',
            # 'encoding': 'utf-8'
        },
    },
    'loggers': {}
}


# Return logger, display INFO level logs in console and record ERROR level logs in file
def get_logger(logger_name=__default_logger, isjob=True):
    if isjob:
        global __default_logger, __log_map
        __default_logger = get_job_name()

        # Set job root logger
        if __default_logger not in __log_map.keys():
            _update_log_config(__default_logger, __default_logger)
            logging.config.dictConfig(LOG_CONFIG)
            ans = logging.getLogger(__default_logger)
            __log_map[__default_logger] = ans
            __log_map['default'] = ans

        if logger_name != __default_logger:
            logger_name = __default_logger + '.' + logger_name
            _update_log_config(logger_name, __default_logger)
            logging.config.dictConfig(LOG_CONFIG)
            ans = logging.getLogger(logger_name)
            __log_map[logger_name] = ans
    else:
        logging.config.dictConfig(LOG_CONFIG)
        ans = logging.getLogger(logger_name)
        __log_map[logger_name] = ans

    return __log_map.get(logger_name)


# Update log config, adding
def _update_log_config(logger_name, default_logger):
    global LOG_CONFIG
    hanlder_config = {
        logger_name: {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'precise',
            'filename': __log_file_path.format(logger_name),
            'mode': 'w',
            'encoding': 'utf-8'
        }
    }

    # Root logger doesn't use console handler
    logger_config = {
        logger_name: {
            'level': 'DEBUG',
            'handlers': [logger_name, 'console'] if logger_name != default_logger else [logger_name],
            'propagate': True,
        }
    }
    global LOG_CONFIG
    LOG_CONFIG['handlers'].update(hanlder_config)
    LOG_CONFIG['loggers'].update(logger_config)



