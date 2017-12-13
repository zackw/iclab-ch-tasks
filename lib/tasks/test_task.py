
import logging

def test_job():
    logging.getLogger("test_job").info("test_job wuz here")

#
# Task interface
#

def add_arguments(ap):
    ap.add_argument("--test", action="store_true", help="test_task wuz here")

def add_jobs(scheduler):
    scheduler.add_job(test_job, trigger="interval", seconds=2,
                      id="test_job", name="test job")
