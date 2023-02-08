"""Example of using ThreadPool Executor wrapper"""

import queue
import uuid

from common.logging import Logger
from common.thread_pool_executor.thread_pool_executor import run_thread_pool_executor

if __name__ == "__main__":
    m_queue = queue.Queue()
    for numb in range(20):
        m_queue.put(list(range(numb + 10)))

    def worker(
        worker_queue: queue.Queue, 
        idx: int    # pylint:disable=unused-argument
    ) -> None:
        """Thread worker"""
        empty_count = 0
        while True:
            if worker_queue.empty():
                if empty_count == 4:
                    break
                empty_count += 1
            else:
                worker_queue.get()
                worker_queue.task_done()

    futures = run_thread_pool_executor(
        workers=[
            (
                worker,
                [
                    m_queue,
                ],
            )
        ],
        worker_replica=5,
    )

    logger = Logger(
        name="DEPLOYMENT",
        level="DEBUG",
        description="DEPLOYMENT",
        trace_id=uuid.uuid4(),
    )

    for futur in futures:
        logger.info(futur.result())
