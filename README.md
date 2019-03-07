# taskerl

Simple task scheduler + worker library.  

Tasks sent to a single worker will get serialized, you can have as many
different workers as you want.

## How to use:

Include it as a dependency, and from your code, call:

```
    {ok, Taskerl} = taskerl:start_link(OnlyAck, QueueLimit),

    WaitingFun = fun(Expected) -> timer:sleep(3000), Expected end,
    taskerl:run(Taskerl, WaitingFun, [1]))
    taskerl:run_async(Taskerl, WaitingFun, [2]))
```

After `QueueLimit` is exhausted, any `:run(` call will return `{taskerl, {error, queue_full}}`
while `:run_async(` will log a warning.

`OnlyAck` is a boolean that makes the underlying queue reply `{taskerl, {ok,
Id}}` when the queue has stored the request instead of blocking the caller
process until the request has been fulfilled.  
The state of a request can be queried with `get_request_status/2`.

## Why does it return `{taskerl, ...}`?

In the case of `run(`, it may return the actual result of the task.

## Can I use the serializer standalone?

Sure! The serializer is able to queue any `gen_server:call` (and transform
`gen_server:cast` to `call`) and send them to a `gen_server` worker, while the
worker itself is doing other tasks (like reconnecting, waiting for some
condition, etc).  
Check the code in [taskerl.erl](src/taskerl.erl) and the first lines of the
[serializer](src/taskerl_gen_server_serializer.erl) to check how to use it.

## TODO:

Add `-spec` everywhere `'-.-`

## Run tests:
```
rebar3 ct
```

## Authors

* **Jose M Perez Ramos** - [Kuroneer](https://github.com/Kuroneer)

## License

[MIT License](LICENSE)

