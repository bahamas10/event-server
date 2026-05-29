...


TODO
----

- CLI support POSTing (creating) events
- remove all "unwrap" and "expect" where appropriate
- put tokio tasks in separate functions (exec program, serialize config, etc.)

Concerns
--------

- timeout command that gets executed on event
- limit `POST` size
- mixture of std::sync and tokio::sync

Possible TODO
--------------

- reload config
- optional datastores
