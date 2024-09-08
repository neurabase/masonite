import pickle
import time
import pendulum
import inspect
from urllib import parse
import traceback
import signal

from ...utils.console import HasColoredOutput

exit_pending: bool = False

def handle_sigterm(signum, frame):
    global exit_pending
    exit_pending = True
    print("Caught SIGTERM... Setting exit flag")

def handle_sigint(signum, frame):
    global exit_pending
    exit_pending = True
    print("Caught SIGINT... Setting exit flag")

signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigint)

class RedisDriver(HasColoredOutput):
    def __init__(self, application):
        self.application = application
        self.connection = None
        self.publishing_channel = None

    def set_options(self, options):
        self.options = options
        return self

    def push(self, *jobs, args=(), **kwargs):
        for job in jobs:
            delay = self.options.get("delay", 0)
            self.info(f"Pushing job {job} to queue {self.options}")
            if delay:
                # Add delay to current unixtime
                delay = int(time.time()) + int(delay)
            payload = {
                "id": 1,  # This is a placeholder for the job id
                "obj": job,
                "args": args,
                "callback": self.options.get("callback", "handle"),
                "created": pendulum.now(tz=self.options.get("tz", "UTC")),
                "schedule_at": delay,
            }

            try:
                self.connect()
                if delay:
                    self.info(f"Job scheduled to run in {delay - int(time.time())} seconds on queue {self.options.get('queue')}")
                    self.connection.zadd(
                        self.options.get("queue") + ":delayed", {pickle.dumps(payload): float(delay)}
                    )
                else:
                    self.info(f"Job scheduled to run immediately on queue {self.options.get('queue')}")
                    self.connection.lpush(self.options.get("queue"), pickle.dumps(payload))
            except Exception as e:
                self.danger(f"Error pushing job to queue: {e}")
                # Print a stack trace
                traceback.print_exc()

    def get_package_library(self):
        try:
            import redis
        except ImportError:
            raise ModuleNotFoundError(
                "Could not find the 'redis' library. Run 'pip install redis' to fix this."
            )

        return redis

    def connect(self):
        try:
            import redis
        except ImportError:
            raise ModuleNotFoundError(
                "Could not find the 'redis' library. Run 'pip install redis' to fix this."
            )
        if self.connection is None:
            self.connection = redis.Redis(host=self.options.get("host"), port=self.options.get("port"), password=self.options.get("password"), db=self.options.get("db", 0), decode_responses=False)

        return self

    def _reschedule_delayed_jobs(self):
        self.connect()
        jobs = self.connection.zrangebyscore(
            self.options.get("queue") + ":delayed", 0, int(time.time())
        )

        for job in jobs:
            self.info(f"Rescheduling job {job} to run immediately from delayed queue to {self.options.get('queue')}")
            self.connection.lpush(self.options.get("queue"), job)
            self.connection.zrem(self.options.get("queue") + ":delayed", job)

    def consume(self):
        self.success(
            '[*] Waiting to process jobs on the "{}" queue. To exit press CTRL+C'.format(
                self.options.get("queue")
            )
        )

        self.connect()
        while True:
            self._reschedule_delayed_jobs()
            payload = self.connection.rpop(self.options.get("queue"))
            if payload is None:
                if exit_pending:
                    break
                time.sleep(int(self.options.get("poll", 1)))
                continue
            job = pickle.loads(payload)
            obj = job["obj"]
            args = job["args"]
            callback = job["callback"]
            job_id = job["id"]

            try:
                try:
                    if inspect.isclass(obj):
                        obj = self.application.resolve(obj)

                    getattr(obj, callback)(*args)

                except AttributeError:
                    obj(*args)

                self.success(
                    f"[{job_id}][{pendulum.now(tz=self.options.get('tz', 'UTC')).to_datetime_string()}] Job Successfully Processed"
                )
            except Exception as e:
                self.danger(
                    f"[{job_id}][{pendulum.now(tz=self.options.get('tz', 'UTC')).to_datetime_string()}] Job Failed"
                )
                stack_trace = traceback.format_exc()
                self.add_to_failed_queue_table(
                    self.application.make("builder").new(), str(job["obj"]), payload, str(e), stack_trace
                )

                if hasattr(obj, "failed"):
                    getattr(obj, "failed")(job, str(e))
            
            if exit_pending:
                break

    def retry(self):
        builder = (
            self.application.make("builder")
            .new()
            .on(self.options.get("connection"))
            .table(self.options.get("failed_table", "failed_jobs"))
        )

        jobs = builder.get()

        if len(jobs) == 0:
            self.success("No failed jobs found.")
            return

        for job in jobs:
            try:
                self.connect().publish(pickle.loads(job["payload"]))
            except (self.get_connection_exceptions()):
                self.connect().publish(pickle.loads(job["payload"]))

        self.success(f"Added {len(jobs)} failed jobs back to the queue")
        builder.table(self.options.get("failed_table", "failed_jobs")).where_in(
            "id", [x["id"] for x in jobs]
        ).delete()

    def work(self, ch, method, _, body):

        job = pickle.loads(body)
        obj = job["obj"]
        args = job["args"]
        callback = job["callback"]

        try:
            try:
                if inspect.isclass(obj):
                    obj = self.application.resolve(obj)

                getattr(obj, callback)(*args)

            except AttributeError:
                obj(*args)

            self.success(
                f"[{method.delivery_tag}][{pendulum.now(tz=self.options.get('tz', 'UTC')).to_datetime_string()}] Job Successfully Processed"
            )
        except Exception as e:
            self.danger(
                f"[{method.delivery_tag}][{pendulum.now(tz=self.options.get('tz', 'UTC')).to_datetime_string()}] Job Failed"
            )

            getattr(obj, "failed")(job, str(e))
            stack_trace = traceback.format_exc()

            self.add_to_failed_queue_table(
                self.application.make("builder").new(), str(job["obj"]), body, str(e), stack_trace
            )

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def add_to_failed_queue_table(self, builder, name, payload, exception, stack_trace=None):
        builder.table(self.options.get("failed_table", "failed_jobs")).create(
            {
                "driver": "redis",
                "queue": self.options.get("queue", "default"),
                "name": name,
                "connection": self.options.get("connection"),
                "created_at": pendulum.now(
                    tz=self.options.get("tz", "UTC")
                ).to_datetime_string(),
                "exception": exception,
                "payload": payload,
                "stack_trace": stack_trace,
                "failed_at": pendulum.now(
                    tz=self.options.get("tz", "UTC")
                ).to_datetime_string(),
            }
        )

    def length(self):
        self.connect()
        return self.connection.llen(self.options.get("queue")) + self.connection.zcard(self.options.get("queue") + ":delayed")
