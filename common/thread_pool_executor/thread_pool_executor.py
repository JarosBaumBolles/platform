"""Thread executor wrapper"""
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, wait
from typing import Any, Callable, Dict, List, Optional, Tuple


def run_thread_pool_executor(
    workers: List[Tuple[Callable, List[Any]]],
    worker_replica: int = 1,
    wait_on_done: bool = True,
    max_pool_workers: int = 10,
) -> Optional[Dict]:
    """Run the given workers in thread poll executor."""
    futures = {}
    with ThreadPoolExecutor(max_workers=max_pool_workers) as executor:
        for idx, (worker, args) in enumerate(workers, 1):
            for replica_idx in range(1, worker_replica + 1):
                key = f"{idx}_{replica_idx}_{worker.__name__}"
                if replica_idx == 1:
                    args.append(key)
                else:
                    args[-1] = key
                fut = executor.submit(worker, *args)
                futures[fut] = key
        if wait_on_done:
            wait(futures, timeout=1, return_when=ALL_COMPLETED)
    return futures
