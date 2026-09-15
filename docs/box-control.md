# box-control interface

The host never tells the model a sandbox id. `RenderCx::use_sandbox` records a framework-owned `SandboxKey`. Drivers implement `oah_sandbox::SandboxDriver`.

## Local

`oah_sandbox::local::LocalSandbox` runs `bash -lc` under a root directory.
It refuses to start when `OAH_HOSTED=1`. The PRD forbids a host-shell escape on hosted.

## Virtual

`VirtualSandbox` is an in-memory filesystem. `exec` is unsupported. Demo and
tests use this driver when `use_sandbox("virtual")` is set. The six built-in
tools (`read`, `write`, `edit`, `bash`, `grep`, `glob`) bind to it.

## hexuria/box (`box-control`)

`oah_sandbox::boxctl` is the documented contract for `hexuria/box`:

```text
BoxControl
  exec(sandbox_id, cmd) -> { stdout, exit_code }
  read_file(sandbox_id, path) -> text
  write_file(sandbox_id, path, bytes)
```

`BoxSandbox` adapts that control plane to `SandboxDriver`. `MockBoxControl`
covers the contract in-process. A production host passes an HTTP client that
talks to box-control. The driver reports computer-use through
`capabilities().computer_use`.
