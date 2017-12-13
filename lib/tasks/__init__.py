"""Tasks library.  Handles enumeration of tasks, process isolation, etc.
"""

import argparse
import importlib
import logging
import logging.config
import multiprocessing
import os
import pkgutil
import signal
import sys
import threading

import setproctitle
from apscheduler.executors.base import BaseExecutor, run_job


class LogForwarder:
    """Serialize and forward logs from the worker processes.
       Also responsible for configuring logging in the scheduler process."""

    def __init__(self, args):
        self.log_file = args.log_file
        self.log_level = args.log_level
        self.queue = multiprocessing.Queue()
        self.main_logger = logging.getLogger("")
        self.logging_thread = None

    def start(self):
        if self.logging_thread is not None:
            raise RuntimeError("log forwarder was already started")

        if self.log_file is None:
            root_handler = logging.StreamHandler(sys.stderr)
        else:
            root_handler = logging.WatchedFileHandler(self.log_file)

        root_formatter = logging.Formatter(
            '{asctime} {name} {levelname:8s} {message}', style='{')
        root_formatter.default_msec_format = '%s.%03d'
        root_handler.setFormatter(root_formatter)

        self.main_logger.setLevel(getattr(logging, self.log_level))
        self.main_logger.addHandler(root_handler)

        self.logging_thread = threading.Thread(
            target=self._log_forward_thread_fn)
        self.logging_thread.start()

    def stop(self):
        if self.logging_thread is not None:
            self.queue.put(None)
            self.logging_thread.join()
            self.logging_thread = None

    def _log_forward_thread_fn(self):
        while True:
            record = self.queue.get()
            if record is None:
                break
            logging.getLogger(record.name).handle(record)


class ProcessPoolWithStateDirs(BaseExecutor):
    """Customized process-pool executor, with special methods to integrate
       into the event loop, and per-task persistent state directories."""
    def __init__(self, args, log_forwarder):
        super().__init__()
        self._pool        = None
        self._max_workers = args.workers
        self._state_dir   = os.path.abspath(args.state_dir)
        self._log_queue   = log_forwarder.queue
        self._wpt_prefix  = os.path.basename(sys.argv[0]) + " worker: "

    def prepare(self):
        if self._pool is not None:
            raise RuntimeError("worker pool already running")

        # All of the state directories are subdirectories of this directory.
        # The worker processes sit in this directory when they are idle.
        os.makedirs(self._state_dir, exist_ok=True)

        # We use a multiprocessing.Pool instead of a
        # concurrent.futures.ProcessPoolExecutor because this gives us
        # more control over how the worker processes behave.
        self._pool = multiprocessing.Pool(
            self._max_workers,
            self._worker_init)

    def stop(self):
        if self._pool is None:
            return
        self._pool.terminate()
        self._pool.join()
        self._pool = None

    def shutdown(self, wait):
        if self._pool is None:
            raise RuntimeError("worker pool is not running")
        self._pool.close()
        if wait:
            self._pool.join()

    def _do_submit_job(self, job, run_times):
        def success_cb(result):
            self._run_job_success(job.id, result)
        def fail_cb(exc):
            self._run_job_error(job.id, exc,
                                getattr(exc, "__traceback__", None))

        self._pool.apply_async(
            self._worker_run_job, (job, run_times), {}, success_cb, fail_cb)

    def _worker_init(self):
        os.chdir(self._state_dir)
        setproctitle.setproctitle(self._wpt_prefix + "idle")

        # We have to use dictConfig because the "disable_existing_loggers"
        # mechanism isn't exposed any other way.  It might not be strictly
        # necessary to do that, but it seems wisest.  All filtration will
        # happen in the parent process.
        logging.config.dictConfig({
            "version": 1,
            "disable_existing_loggers": True,
            "handlers": {
                "queue": {
                    "class": "logging.handlers.QueueHandler",
                    "queue": self._log_queue
                }
            },
            "root": {
                "level": "DEBUG",
                "handlers": ["queue"]
            }
        })

        # We ignore SIGINT and SIGHUP but not SIGTERM in workers,
        # because Pool.terminate sends SIGTERMs.
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGHUP, signal.SIG_IGN)

        logging.getLogger("").debug("Worker ready")


    def _worker_run_job(self, job, run_times):
        os.makedirs(job.id.replace("/", "-"), exist_ok=True)
        try:
            setproctitle.setproctitle(self._wpt_prefix + job.name)
            os.chdir(job.id)
            return run_job(job, job._jobstore_alias, run_times,
                           "task-" + job.id)
        finally:
            os.chdir(self._state_dir)
            setproctitle.setproctitle(self._wpt_prefix + "idle")


def _call_for_all_task_modules(name, *args, **kwargs):
    for importer, modname, ispkg in pkgutil.walk_packages(
            __path__, __name__+'.'):
        mod = importlib.import_module(modname)
        getattr(mod, name)(*args, **kwargs)


def add_arguments(ap):
    # Generic arguments

    VALID_LOG_LEVELS = sorted(
        (x for x in dir(logging)
         if x == x.upper() and x[0] != "_"
         and x not in ("BASIC_FORMAT", "NOTSET", "WARNING", "FATAL")),
        key=lambda x: -getattr(logging, x))

    NWORKERS_DEFAULT = os.cpu_count() - 1

    def positive_int(val):
        val = int(val)
        if val <= 0:
            raise argparse.ArgumentTypeError(
                "'{}': not a positive integer".format(val))
        return val

    def case_insensitive(val):
        return val.upper()

    ap.add_argument("--log-file", metavar="FILE",
                    help="File to write logs to (default: stderr)")

    ap.add_argument("--log-level", metavar="LEVEL", default="WARN",
                    choices=VALID_LOG_LEVELS,
                    type=case_insensitive,
                    help="Select logging level (default: WARN, choices: "
                    + ", ".join(VALID_LOG_LEVELS) + ")")

    ap.add_argument("--workers", metavar="N",
                    type=positive_int, default=NWORKERS_DEFAULT,
                    help="Number of worker processes (min: 1, default: {})"
                    .format(NWORKERS_DEFAULT))

    ap.add_argument("--state-dir", metavar="DIR", default="task-state",
                    help="Directory in which to store persistent task state.")

    _call_for_all_task_modules('add_arguments', ap)


def add_jobs(scheduler):
    _call_for_all_task_modules('add_jobs', scheduler)
