"""
Poolboy library

DebugThread class:
    thread used for debugging purposes

Executor Abstract
"""
from abc import ABC, abstractmethod, abstractproperty
from queue import Queue, Empty
from threading import RLock, Lock, Thread

# =========================
# UTILS
# =========================

class Error(Exception):
    pass

class NotImplementedError(Error):
    pass

# ==================================================
# RUNNABLES
# ==================================================

class AbstractFactoryRunnable(ABC):
    """
    Allow user to give his own implementation of a runnable.
    """

    @abstractmethod
    def __init__(self, *args, **kwargs):
        """
        Override the init method to suit your needs.
        """

    @abstractmethod
    def get_runnable(self):
        pass

# =========================================
# WORKERS
# =========================================

class AbstractFactoryWorker(ABC):
    """
    Allow user to give his own implementation of a worker.
    """
    @abstractmethod
    def __init__(self, *args, **kwargs):
        """
        Provide your own init method to suit your needs
        """

    @abstractmethod
    def get_worker(self, *args, **kwargs):
        """
        Method that will be called inside of the Executor class.
        """

class BaseWorker(Thread):
    """
    Abstract worker thread
    =======================
    members:
        name: unique name assigned to the thread
               default: python implementation
        
        id: id assigned to the thread by the OS

        _task_queue = reference to the task queue owned by the Executor
        _poll_timeout: max thread sleep
        _terminated: thread has been terminated by the Executor

    ========================
    methods:
        run: run the task dequeued
        terminate: terminate the thread
    """
    def __init__(self, name, task_queue, poll_timeout=1, thread_args=[], thread_kwargs={}, *args, **kwargs):
        super().__init__(name=name, target=self.run, *thread_args, **thread_kwargs)

        self._task_queue = task_queue
        self._poll_timeout = poll_timeout
        self._terminated = False
        

    def __repr__(self):
        return f"""
        {self.__class__.__name__}(task_queue: {self._task_queue.__repr__()},
                   poll_timeout: {self._poll_timeout},
                   id: {self.id},
                   name: {self.name})
        """

    def terminate(self):
        self._terminated = True

    def is_terminated(self):
        return self._terminated

    def __str__(self):
        return f
        """
            {self.__class__.__name} using queue {self._task_queue} with timeout {self._poll_timeout}.
        """

    def run(self):
        """
        While is not terminated:
            get a task from the queue waiting at most _poll_time ms
            run the task
            do all over again.
        """
        raise NotImplementedError("method <run> not implemented.")

class Worker(BaseWorker):
    """
    Implementation of the abstract worker class.
    """
    def __init__(self, 
                 name=None,
                 task_queue=None,
                 poll_timeout=1,
                 thread_args=[],
                 thread_kwargs={},
                 *args,
                 **kwargs):

        super().__init__(
                name=name,
                task_queue=task_queue,
                poll_timeout=poll_timeout,
                thread_args=thread_args,
                thread_kwargs=thread_kwargs,
                *args,
                **kwargs)

    def run(self):
        while self._terminated is False:
            try:
                task = self._task_queue.get(block=True, timeout=self._poll_timeout)
                if not isinstance(task, AbstractRunnable):
                    raise ValueError(f"task put into the queue is not a Runnable, {task.__repr__()}")

                # run the task calling the __call__ method of the instance
                task()

                # notify the queue that the task has been completed 
                self._task_queue.task_done()

            except Empty:
                continue

class BaseExecutor:
    def __init__(self, 
                 num_workers: int =4,
                 poll_timeout: int = 1, 
                 put_timeout: int = 1,
                 task_queue=None,
                 worker_factory=None,
                 thread_args=[],
                 thread_kwargs=[],
                 *args,
                 **kwargs
        ):

        self._num_workers = num_workers
        self._worker_factory = worker_factory
        self._worker_class = self._get_worker_class()
        self._task_queue = task_queue
        self._thread_args = thread_args
        self._thread_kwargs = thread_kwargs
        self._workers = [self._worker_class(task_queue=self._task_queue, poll_timeout=poll_timeout, *thread_args, **thread_kwargs)]

        self._poll_timeout = poll_timeout
        self._put_timeout = put_timeout
        self._start_threads()


    def _start_threads(self):
        for worker in self._workers:
            worker.start()

    def _get_worker_class(self):
        if self._worker_factory is None:
            return Worker

        return self._worker_factory.get_worker()

    def put_task(self, task):
        """
        Put a task of class AbstractRunnable or derived inside of the queue
        """
        raise NotImplementedError("method <put_task> not implemented")

    def terminate(self):
        return self._joinall()
    
    def empty_queue(self):
        return self._task_queue.join()

    def terminate_workers(self):
        for w in self._workers:
            w.terminate()

    def _joinall(self):
        self.empty_queue()
        self.terminate_workers()

    @property
    def pool_size(self):
        """
        Return the size of the pool
        """
        return self.num_workers

class ThreadPoolExecutor(BaseExecutor):
    def __init__(self, 
                 num_workers=2, 
                 poll_timeout=1, 
                 put_timeout=1,
                 task_queue=None, 
                 thread_args: list = [],
                 thread_kwargs: dict = {},
                 worker_factory=None):

        super().__init__(
                num_workers=num_workers, 
                poll_timeout=poll_timeout,
                put_timeout=put_timeout,
                task_queue=task_queue,
                worker_factory=worker_factory,
                thread_args=thread_args,
                thread_kwargs=thread_kwargs
       )


    def put_task(self, task):
        """
        Put a task of class AbstractRunnable or derived inside of the queue
        """
        if not isinstance(task, AbstractRunnable):
            raise ValueError(f"Task is not a Runnable: {task.__repr__()}")
        
        self._task_queue.put(item=task, block=True, timeout=self._put_timeout)

    def _put_task(self, task):
        """
        Put a task returned by put_cb
        """
        return self._task_queue.put(item=task, block=True, timeout=self._put_timeout)

    def put_cb(self, cb, *args, **kwargs):
        runnable = ExecutorTask(cb, *args, **kwargs)
        return self._put_task(runnable)

class AbstractRunnable(ABC):
    """
    Abstract class for the runnable object.
    This is the class of a task pushed onto the queue
    """

    @abstractmethod
    def __init__(self, cb: callable, *args, **kwargs):
        self.fn = cb
        self.args = args
        self.kwargs = kwargs

    @abstractmethod
    def execute(self):
        """
        Function that gets called inside of Worker.run method.
        """
        return self.fn(*self.args, **self.kwargs)
    
    @abstractmethod
    def __call__(self):
        """
        This object should also behave as a callable.
        """
        pass

class ExecutorTask(AbstractRunnable):
    def __init__(self, cb: callable, *args, **kwargs):
        self.fn = cb
        self.args = args
        self.kwargs = kwargs

    def execute(self):
        return self.fn(*self.args, **self.kwargs)
    
    def __call__(self):
        return self.execute()
