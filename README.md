...


TODO
----

- CLI support GETing event-stream
- event stream supporting backlog of events
- good logging
- better & consistent naming - should this tool have its own name?
- move code into cargo workspaces (maybe)
  - separate client from server with shared library
- (maybe) log events to a single file as newline-separated JSON

Concerns
--------

- timeout command that gets executed on event
- limit `POST` size

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
