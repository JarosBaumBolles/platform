"""Internall logging"""
import json
import logging
import warnings
from collections.abc import MutableMapping
from copy import deepcopy
from functools import reduce
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from google.cloud.logging_v2.handlers import StructuredLogHandler

import common.settings as CFG
from common.cache import lru_cache_expiring
from queue import Queue

# Exclude internal logs from propagating through handlers
EXCLUDED_LOGGER_DEFAULTS = (
    "google.cloud",
    "google.auth",
    "google_auth_httplib2",
    "google.api_core.bidi",
    "werkzeug",
)


@lru_cache_expiring(maxsize=512, expires=3600)
def get_logger(
    name: str = CFG.LOGGER_NAME,
    level=CFG.LOGGER_LEVEL,
    exluded_logger: Optional[Union[List, Tuple, Set]] = None,
) -> Union[logging.StreamHandler, StructuredLogHandler]:
    """Get logger instance."""
    exluded_logger = exluded_logger or ()
    if CFG.DEBUG and CFG.LOCAL_RUN:
        color_formatter = StreamColorFormatter(CFG.LOCAL_LOGGER_FORMAT)
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(color_formatter)
    else:
        handler = StructuredLogHandler()

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    exluded_logger = tuple(exluded_logger) + EXCLUDED_LOGGER_DEFAULTS
    for lg_name in exluded_logger:
        # prevent excluded loggers from propagating logs to handler
        lggr = logging.getLogger(lg_name)
        lggr.propagate = False

    return logger


class StreamColorFormatter(logging.Formatter):
    """Custom logger formatter to colorize output"""

    grey = "\x1b[38;21m"
    blue = "\x1b[38;5;39m"
    yellow = "\x1b[38;5;226m"
    red = "\x1b[38;5;196m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"

    def __init__(self, fmt):
        super().__init__()
        self.fmt = fmt
        self.formats = {
            logging.DEBUG: f"{self.grey}{self.fmt}{self.reset}",
            logging.INFO: f"{self.blue}{self.fmt}{self.reset}",
            logging.WARNING: f"{self.yellow}{self.fmt}{self.reset}",
            logging.ERROR: f"{self.red}{self.fmt}{self.reset}",
            logging.CRITICAL: f"{self.bold_red}{self.fmt}{self.reset}",
        }

    def format(self, record):
        log_fmt = self.formats.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


class Logger:
    """Logger instance"""

    __stream_msg_format__ = "[{transaction_id}] [{description}] - {message}; {extra}"

    def __init__(
        self,
        name: str = CFG.LOGGER_NAME,
        level: str = CFG.LOGGER_LEVEL,
        description: str = "",
        trace_id: str = "",
    ) -> None:
        self._logger = get_logger(name=name, level=level)
        self._extra = {
            "labels": {
                "description": description,
                "trace_id": str(trace_id),
            },
        }

    def _rec_merge(
        self, dict_1: Dict[str, Any], dict_2: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Update two nested dicts recursively,
        if either mapping has leaves that are non-dicts,
        the second's leaf overwrites the first's.
        """
        for key, value in dict_1.items():
            if key in dict_2:
                # this next check is the only difference!
                if all(isinstance(e, MutableMapping) for e in (value, dict_2[key])):
                    dict_2[key] = self._rec_merge(value, dict_2[key])
                # we could further check types and merge as appropriate here.
        dict_3 = dict_1.copy()
        dict_3.update(dict_2)
        return dict_3

    def _merge_dicts(self, dicts: Tuple[Dict[str, Any]]) -> Dict[str, Any]:
        try:
            return reduce(self._rec_merge, dicts)
        except TypeError:
            return deepcopy(self._extra)

    def _stream_log(self, message: str, log_handler: Callable, **kwargs) -> None:
        if callable(log_handler):
            extra = dict(
                filter(
                    lambda x: x not in ("message", "asctime"),
                    kwargs.get("extra", {}).items(),
                )
            )

            if extra:
                extra_str = json.dumps(extra, sort_keys=True, indent=2)
            else:
                extra_str = ""

            msg = self.__stream_msg_format__.format(
                transaction_id=self._extra["labels"]["trace_id"],
                description=self._extra["labels"]["description"],
                message=message,
                extra=extra_str,
            )
            log_handler(msg)

    def _struct_log(self, message: str, log_handler: Callable, **kwargs) -> None:
        if callable(log_handler):
            extra = self._merge_dicts((self._extra, kwargs.get("extra", {})))
            extra = dict(
                filter(lambda x: x not in ("message", "asctime"), extra.items())
            )
            log_handler(message, extra=extra)

    def log(self, level: Union[str, int], message: str, *args, **kwargs) -> None:
        """
        Log 'msg % args' with given severity.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.
        """
        if isinstance(level, str):
            level = level.strip().upper()

        level_name = logging.getLevelName(level)
        if isinstance(level_name, int):
            level_name = level.strip()

        if not level_name.startswith("Level "):
            log_handler = getattr(self._logger, level_name.strip().lower())
            if CFG.DEBUG and CFG.LOCAL_RUN:
                self._stream_log(
                    message=message, log_handler=log_handler, *args, **kwargs
                )
            else:
                self._struct_log(
                    message=message, log_handler=log_handler, *args, **kwargs
                )

    def debug(self, msg, *args, **kwargs):
        """
        Log 'msg % args' with severity 'DEBUG'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.debug("Houston, we have a %s", "thorny problem", exc_info=1)
        """

        self.log(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        """
        Log 'msg % args' with severity 'INFO'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.info("Houston, we have a %s", "interesting problem", exc_info=1)
        """
        self.log(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        """
        Log 'msg % args' with severity 'WARNING'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.warning("Houston, we have a %s", "bit of a problem", exc_info=1)
        """
        self.log(logging.WARNING, msg, *args, **kwargs)

    def warn(self, msg, *args, **kwargs):
        """Deprecated Method"""
        warnings.warn(
            "The 'warn' method is deprecated, " "use 'warning' instead",
            DeprecationWarning,
            2,
        )
        self.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        """
        Log 'msg % args' with severity 'ERROR'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.error("Houston, we have a %s", "major problem", exc_info=1)
        """
        self.log(logging.ERROR, msg, *args, **kwargs)

    def exception(self, msg, *args, exc_info=True, **kwargs):
        """
        Convenience method for logging an ERROR with exception information.
        """
        self.error(msg, *args, exc_info=exc_info, **kwargs)

    def critical(self, msg, *args, **kwargs):
        """
        Log 'msg % args' with severity 'CRITICAL'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.critical("Houston, we have a %s", "major disaster", exc_info=1)
        """
        self.log(logging.CRITICAL, msg, *args, **kwargs)

    def fatal(self, msg, *args, **kwargs):
        """
        Don't use this method, use critical() instead.
        """
        self.critical(msg, *args, **kwargs)



class ThreadPoolExecutorLogger:
    def __init__(
        self,
        description: str = "",
        trace_id: str = "",
    ) -> None:
        self._queue = Queue()
        self._trace_id = trace_id
        self._logger = Logger(
            description=description,
            trace_id=trace_id,            
        )

    def debug(self, msg, *args, **kwargs):
        """
        Log 'msg % args' with severity 'DEBUG'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.debug("Houston, we have a %s", "thorny problem", exc_info=1)
        """

        self.log(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        """
        Log 'msg % args' with severity 'INFO'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.info("Houston, we have a %s", "interesting problem", exc_info=1)
        """
        self.log(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        """
        Log 'msg % args' with severity 'WARNING'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.warning("Houston, we have a %s", "bit of a problem", exc_info=1)
        """
        self.log(logging.WARNING, msg, *args, **kwargs)

    def warn(self, msg, *args, **kwargs):
        """Deprecated Method"""
        warnings.warn(
            "The 'warn' method is deprecated, " "use 'warning' instead",
            DeprecationWarning,
            2,
        )
        self.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        """
        Log 'msg % args' with severity 'ERROR'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.error("Houston, we have a %s", "major problem", exc_info=1)
        """
        self.log(logging.ERROR, msg, *args, **kwargs)

    def exception(self, msg, *args, exc_info=True, **kwargs):
        """
        Convenience method for logging an ERROR with exception information.
        """
        self.error(msg, *args, exc_info=exc_info, **kwargs)

    def critical(self, msg, *args, **kwargs):
        """
        Log 'msg % args' with severity 'CRITICAL'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.critical("Houston, we have a %s", "major disaster", exc_info=1)
        """
        self.log(logging.CRITICAL, msg, *args, **kwargs)

    def fatal(self, msg, *args, **kwargs):
        """
        Don't use this method, use critical() instead.
        """
        self.critical(msg, *args, **kwargs)       


    def log(self, level: Union[str, int], message: str) -> None:
        """
        Log 'msg % args' with given severity.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.
        """
        if isinstance(level, str):
            level = level.strip().upper()

        level_name = logging.getLevelName(level)
        if isinstance(level_name, int):
            level_name = level.strip()

        if not level_name.startswith("Level "):
            self._queue.put((level, self._trace_id, message))

    def flush_logs(self) -> None:
        while not self._queue.empty():
            lvl, trace_id, msg = self._queue.get()
            self._logger.log(level=lvl, message=msg)
            self._queue.task_done()