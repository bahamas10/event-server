...


TODO
----

- better format events for the CLI (color?)
- event stream supporting backlog of events
- better & consistent naming - should this tool have its own name?
- move code into cargo workspaces (maybe)
  - separate client from server with shared library
- (maybe) log events to a single file as newline-separated JSON

Concerns
--------

- timeout command that gets executed on event
- limit `POST` size
- log level is silently discarded and logging is disabled if it is unknown

Possible TODO
--------------

- reload config
- optional datastores

Usage
-----

```
$ cli tail [-f]
$ cli create [--component=CLI] [--level=info] 'message here'
```
