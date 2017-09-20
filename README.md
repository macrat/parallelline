# Parallel Line

A Framework for buffering heavy calculation (or I/O) with multiprocessing for Python.

## Demo
``` shell
$ git clone http://github.com/macrat/parallelline
$ cd parallelline
$ python3 parallelline.py
```

## Usage
Make something Task. The Task is a function that takes one Pipe instance.

``` python
>>> def load_task(pipe: Pipe) -> None:
...     for i in range(10):
...         with open('{}.dat', 'rb') as f:
...             pipe.put(f.read())
```

The Task function can replace with a generator. Please use `@generator_task` decorator.
The pipe in arguments is Pipe instance but can use as an iterator.
Of course, can use Pipe as an iterator in the normal Task function.

``` python
>>> @generator_task
... def something_process(pipe: Pipe) -> typing.Iterator:
...     for data in pipe:
...         yield something_heavy_process(data)
```

You can use `@task` decorator if Task hasn't to hold state.
Please make a function that takes one object and return one object, and allies `@task` decorator. In this example, show function will return None but work well.

``` python
>>> @task
... def show(data: typing.Any) -> typing.Any:
...     print(data)
```


At last, concatenate all tasks by Pipeline class, and start processes.
The Pipeline will do coloring all messages that wrote into stdio by tasks.

``` python
>>> Pipeline(load_task, something_heavy_process, show).start()
```

## License
MIT License
