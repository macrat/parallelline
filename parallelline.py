""" A Framework for buffering heavy calculation (or I/O) with multiprocessing


Make something Task. The Task is a function that takes one Pipe instance.
>>> def load_task(pipe: Pipe) -> None:
...     for i in range(10):
...         with open('{}.dat', 'rb') as f:
...             pipe.put(f.read())

The Pipe can replace with a generator. Please use @generator_task decorator.
The pipe in arguments is Pipe instance but can use as an iterator. Of course,
can use Pipe as an iterator in the normal Task function.
>>> @generator_task
... def something_process(pipe: Pipe) -> typing.Iterator:
...     for data in pipe:
...         yield something_heavy_process(data)

You can use @task decorator if Task hasn't to hold state.
Please make a function that takes one object and return one object, and allies
@task decorator. In this example, show function will return None but work well.
>>> @task
... def show(data: typing.Any) -> typing.Any:
...     print(data)


At last, concatenate all tasks by Pipeline class, and start processes.
Pipeline class will be coloring messages that wrote into stdio by tasks.
>>> Pipeline(load_task, something_heavy_process, show).start()
"""

import functools
import io
import math
import multiprocessing
import queue
import sys
import time
import traceback
import typing


class _PipeIterator(typing.Iterator):
    def __init__(self, pipe: 'Pipe') -> None:
        self._pipe = pipe

    def __next__(self) -> typing.Any:
        return self._pipe.get()


class TaskClosed(StopIteration):
    """ An exception that means task was closed. """
    pass


class Pipe(typing.Iterable):
    """ The pipe for communication with other Task

    A pipe has two queues that for send and for receive.
    """

    def __init__(self, input_: typing.Any, output: typing.Any) -> None:
        """
        input_ -- A queue for receive message from other tasks.
        output -- A queue for send message from other tasks.
        """

        self._input = input_
        self._input_closed = multiprocessing.Value('b', False)

        self._output = output
        self._output_closed = multiprocessing.Value('b', False)

    def get(self, timeout: typing.Optional[int] = None) -> typing.Any:
        """ Get data from other tasks

        Will be raising TaskClosed exception if already closed queue for
        receiving by a sender task.


        timeout -- Time limit of waiting for new data. Will block until
                   receiving new data if None. Won't waits if 0 or less.

        return -- Received data.
        """

        if self._input is None:
            return None

        if self._input_closed.value:
            raise TaskClosed()

        value = self._input.get(block=timeout is None or timeout > 0,
                                timeout=timeout)
        if value is TaskClosed:
            self._input_closed.value = True
            raise TaskClosed()

        return value

    def put(self, value: typing.Any) -> None:
        """ Put data to other tasks

        This method will block until put data done.

        Will be raising TaskClosed exception if call this after pipe closed.


        value -- Something data for send.
        """

        if self._output_closed.value:
            raise TaskClosed()

        if self._output is not None:
            self._output.put(value)

    def __iter__(self) -> typing.Iterator:
        """ Get an iterator for getting data from other tasks """

        return _PipeIterator(self)

    def close(self) -> None:
        """ Close queue for sending

        Can't put data anymore after called this.
        But you can get data from other tasks even if pipe closed.
        """

        if self._output is not None:
            self._output.put(TaskClosed)
            self._output.close()
        self._output_closed.value = True

    @property
    def closed(self) -> bool:
        """ Checking this Pipe is closed.

        Please see close method.
        """

        return self._output_closed.value


class _LogQueueWriter(io.TextIOBase):
    """ Log handler for Pipeline """

    def __init__(self, id_: typing.Any, queue: multiprocessing.Queue) -> None:
        self.id_ = id_
        self._queue = queue
        self._buffer = ''

    def readable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    def write(self, message: str) -> int:
        self._buffer += message

        for line in self._buffer.split('\n')[:-1]:
            self._queue.put((self.id_, line))

        self._buffer = self._buffer.split('\n')[-1]

        return len(message)

    def flush(self) -> None:
        for line in self._buffer.split('\n'):
            self._queue.put((self.id_, line))
        self._buffer = ''

    def close(self) -> None:
        self.flush()
        self._queue.close()


Task = typing.Callable[[Pipe], None]


def task(func: typing.Callable[[typing.Any], typing.Any]) -> Task:
    """ Decorator for make simple Task

    This decorator is expected used for work that like simple transform.


    func -- The function for decoration. Takes one object from other tasks,
            and return one object for other tasks.
    """

    def functional_task(pipe: Pipe) -> None:
        while True:
            try:
                try:
                    input_ = pipe.get()
                except TaskClosed:
                    pipe.close()
                    return

                pipe.put(func(input_))
            except Exception:
                traceback.print_exc(file=sys.stderr)

    functional_task.__name__ = func.__name__
    functional_task.__doc__ = func.__doc__

    return functional_task


def first_task(func: typing.Callable[[], typing.Any]) -> Task:
    """ Decorator for make first Task

    This decorator is expected used for work that generating some data.


    func -- The function for decoration. Takes no any argument and return one
            object for other tasks.
    """

    def functional_first_task(pipe: Pipe) -> None:
        while True:
            try:
                pipe.put(func())
            except TaskClosed:
                pipe.close()
                return
            except Exception:
                traceback.print_exc(file=sys.stderr)

    functional_first_task.__name__ = func.__name__
    functional_first_task.__doc__ = func.__doc__

    return functional_first_task


def generator_task(func: typing.Callable[[Pipe], typing.Iterator]) -> Task:
    """ Decorator for make Task from a generator


    func -- The generator function for decoration. Takes one Pipe instance and
            yield objects for other tasks.
    """

    def generator_task_wrapper(pipe: Pipe) -> None:
        for x in func(pipe):
            pipe.put(x)
        pipe.close()

    generator_task_wrapper.__name__ = func.__name__
    generator_task_wrapper.__doc__ = func.__doc__

    return generator_task_wrapper


class Pipeline:
    """ Pipeline for concatenating tasks

    Tasks will execute in other processes.
    """

    def __init__(self, *tasks: Task, queue_size: int = 100) -> None:
        """
        tasks      -- Tasks for concatenate.
        queue_size -- The maximum number of data in the queue between tasks.
        """

        self.tasks = tasks

        self._task_colors = ['{0}'.format((i+1)%7 + 1)
                             for i in range(len(tasks))]

        self._logger = multiprocessing.Queue()

        self._queue_size = queue_size
        self._queues = [multiprocessing.Queue(queue_size)
                        for _ in range(len(tasks)-1)]

        self._pipes = ([Pipe(None, self._queues[0])]
                       + [Pipe(self._queues[i], self._queues[i+1])
                          for i in range(0, len(self._queues)-1)]
                       + [Pipe(self._queues[-1], None)])

    def start(self) -> None:
        """ Start all tasks and show status """

        procs = []
        for i, (t, p) in enumerate(zip(self.tasks, self._pipes)):
            procs.append(multiprocessing.Process(
                target=self._task_wrapper,
                args=(i, t, p),
            ))
            procs[-1].start()

        qsize_format = '{{:{0}}}'.format(math.floor(math.log10(self._queue_size)) + 1)

        while True:
            try:
                for i in range(100):
                    try:
                        (id_, type_), msg = self._logger.get_nowait()
                    except queue.Empty:
                        break

                    if type_ == 'stdout':
                        print(('\033[2A\033[1;4' + self._task_colors[id_] + 'm{0}\033[0m {1}\033[K').format(self.tasks[id_].__name__, msg))
                    else:
                        print(('\033[2A\033[1;4' + self._task_colors[id_] + 'm{0}\033[39;41m {1}\033[0m\033[K').format(self.tasks[id_].__name__, msg), file=sys.stderr)
                    print('\033[K')
                    print('')

                closed = [p.closed for p in self._pipes]

                print(('\033[1A\033[' + ('3' if closed[0] else '1;4') + self._task_colors[0] + 'm{0}\033[0m{1}\033[K').format(
                    self.tasks[0].__name__,
                    ''.join(
                        ' =[{0}]=> {1}'.format(
                            (('\033[31m' if q.full() else '') + qsize_format + '/' + qsize_format + '\033[0m').format(q.qsize(), self._queue_size),
                            '\033[' + ('3' if closed[i+1] else '1;4') + self._task_colors[i+1] + 'm{}\033[0m'.format(t.__name__)
                        )
                        for i, (q, t) in enumerate(zip(self._queues, self.tasks[1:]))
                    ),
                ))

                time.sleep(0.1)

                if all(closed) and self._logger.empty():
                    break
            except KeyboardInterrupt:
                for p in self._pipes:
                    p.close()

    def _task_wrapper(self, id_: int, task: Task, pipe: Pipe):
        """ Set stdio hook and execute task

        BE CAREFUL: sys.stdout and sys.stderr will override and never restore.
        """

        sys.stdout = _LogQueueWriter((id_, 'stdout'), self._logger)
        sys.stderr = _LogQueueWriter((id_, 'stderr'), self._logger)

        try:
            task(pipe)
        except TaskClosed:
            pipe.close()
        except Exception:
            traceback.print_exc(file=sys.stderr)
            pipe.close()


if __name__ == '__main__':
    import time

    @generator_task
    def zero(pipe):
        for i in range(10):
            print('zero')
            yield 0
        print('stop')

    @task
    def inc(x):
        print(x)
        time.sleep(x*0.1)
        return x + 1

    @generator_task
    def times(pipe):
        try:
            for x in pipe:
                print(x * 2)
                yield x * 2
        except KeyboardInterrupt as e:
            print('interrupt')
        finally:
            print('finalize')

    Pipeline(zero, inc, inc, inc, inc, inc, times, queue_size=10).start()
