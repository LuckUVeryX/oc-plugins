Use dedicated task worktrees under the primary checkout's `.worktrees/`.

This repo is greenfield. Make breaking changes directly without migrations, deprecations or compatibility shims.

Write a test only when a regression would break a user or agent and go unnoticed: a contract other code or agents depend on, a security or privacy boundary, or a bug that already happened. Leave schema validation, config parsing and library behaviour to the libraries that own them. When a test stops meeting this bar, delete it.
