from threading import Event, Thread
from typing import Callable, Optional, List
from dataclasses import dataclass
from datetime import datetime, timedelta

import logging

from croniter import croniter
from pytz import timezone as tz


@dataclass
class Task:
    id: str
    cron_expression: str
    func: Callable
    last_execution: Optional[datetime]
    next_execution: Optional[datetime]


@dataclass
class CronMachineConfig:
    timezone: str


class CronMachine:
    def __init__(self, config: CronMachineConfig):
        self.log = logging.getLogger(__name__)
        self._stopped: bool = False
        self._event: Event = Event()
        self._tasks: List[Task] = []
        self._tz = tz(config.timezone)

    def add_task(self, id: str, cron_expression: str, task: Callable) -> None:
        self._tasks.append(Task(id, cron_expression, task, None, None))

    def start(self, detach=True) -> None:
        if detach:
            thread = Thread(
                name="cron-machine-thread",
                target=lambda: self.start(detach=False)
            )
            thread.start()
            self.log.info("Cron machine will start in background")
            return
        now = self._tz.localize(datetime.now())
        for t in self._tasks:
            t.last_execution = now

        self.log.info("Cron machine started")

        while not self._stopped:
            for t in self._tasks:
                t.next_execution = croniter(t.cron_expression, t.last_execution).get_next(datetime)

            if self._tasks:
                earlier_execution = min(map(lambda t: t.next_execution, self._tasks))
            else:
                self.log.warning("Task list for cron machine is empty. Wait for 1 minute")
                earlier_execution = now + timedelta(minutes=1)

            now = self._tz.localize(datetime.now())
            time_to_sleep = (earlier_execution - now).total_seconds()
            if time_to_sleep > 0:
                self._event.wait(timeout=time_to_sleep)
                if self._stopped:
                    break
            else:
                self.log.warning(f"No wait time, possible bug")

            now = self._tz.localize(datetime.now())
            tasks_to_execute = filter(lambda t: t.next_execution <= now, self._tasks)
            for t in tasks_to_execute:
                try:
                    t.func()
                except Exception as e:
                    self.log.exception(f"Task with id='{t.id}' failed:", exc_info=e)
                finally:
                    t.last_execution = now

        self.log.info("Cron machine stopped")

    def stop(self):
        self._stopped = True
        self._event.set()
