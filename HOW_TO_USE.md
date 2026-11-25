# tinyq CLI

## Run a job

```console
$ tinyq enqueue 'Summarize notes.md'
{"id":1}
$ tinyq next
{"id":1,"payload":"Summarize notes.md","receipt":"/tmp/tinyq-claim-123.json","expires_at":"2026-11-24T12:05:00Z"}
```

The first command starts the local server if needed. `next` claims one job, or prints `null` if none is waiting. Each claim gets its own receipt file, even when two agents run from the same directory.

After saving the summary, pass the **receipt path from your own `next` output** to `done`:

```console
$ tinyq done /tmp/tinyq-claim-123.json
{"ok":true}
```

`done` removes the receipt. If the work fails, skip `done`: another worker can pick up the job when the claim expires. A crash between saving the result and calling `done` can cause a retry. For work that must run only once, include a task key in the payload and check whether its result already exists.

Claims last five minutes by default. Use `tinyq next --ttl 10m` to claim a job for longer, or `tinyq renew PATH` before your current claim expires. Renewal gives you the original duration again. A `409 Conflict` means the claim is no longer yours; stop and discard the receipt.

## Use a separate queue

```sh
export TINYQ_PARTITION=research
tinyq enqueue - < job.json
tinyq stats
```

Agents that exchange jobs should use the same partition. If you don't set one, tinyq uses `default`. To select a partition for one command, put the flag first: `tinyq --partition writing next`.

`tinyq --partition research empty` permanently clears `research`, including claimed jobs. It resets the counts and starts job IDs at 1 again. Other partitions are untouched.

## Run a worker program

```sh
tinyq work -- ./worker
```

This runs `./worker` once with the job's bytes on stdin. tinyq renews the claim while the program runs and marks the job done only if it exits successfully. If the queue is empty, it prints `null` without starting the program.

## Scripting notes

Run `tinyq stop` to gracefully stop the local server for all partitions. Unfinished jobs survive, and the next local queue command starts the server again. If the server is already stopped, `tinyq stop` succeeds without starting it.

- Queue commands print one JSON value to stdout on success. Check the exit code before parsing it. Errors and worker program output go to stderr.
- `enqueue` accepts up to 1 MiB. Use `enqueue -` for files, pipes, or multiline input. `next` returns UTF-8 payloads as text; other bytes come back with `"encoding":"base64"`.
- The local server listens on `127.0.0.1:8080` and stops after 15 minutes without requests. Unfinished jobs survive a restart. To change the timeout, set `TINYQ_IDLE_TIMEOUT` before starting the server (`30m`, for example, or `0` to keep it running). For long claims, renew before the idle timeout as well as before expiry.
- Set `TINYQ_URL` to use another server or `TINYQ_WAL` to choose where the local server stores jobs. Run `tinyq --help` for all commands and `tinyq serve -h` for server settings.
