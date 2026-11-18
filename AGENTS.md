# Repository working instructions

## Spark-only checkout

- This checkout is `/share/gezhilong/trajfoundry-spark`, on branch `spark-tokenplan`. Develop and test the independent Spark implementation here.
- The ordinary Python deployment is `/share/gezhilong/trajfoundry`, imported by `/share/envs/gzl_trajfoundry_env/bin/python`. Do not modify that checkout, environment, or its scheduler scripts during Spark work.
- Use this checkout's `.venv` and explicitly verify that `trajfoundry.__file__` resolves inside this checkout; both installations share the same package name.
- Do not edit, commit, or push from `/home/gezhilong/TrajFoundry/trajfoundry`. It is an older, separate clone and is not the code used by DolphinScheduler.
- Before changing code, verify `pwd`, `git rev-parse --show-toplevel`, and `git branch --show-current` identify this Spark checkout. Do not commit or push without the user's request.

## Scheduler and storage safety

- Jupyter Client mode is for development; DolphinScheduler SHELL + Spark Cluster mode is for production. The platform documents different `/share` views on the scheduler Worker and in Kubernetes; verify paths in the actual Driver/Executor environment.
- Do not modify Spark source files while a Spark business task is running from this checkout.
- Keep x86 Driver and ARM Executor Python runtimes separate with matching major/minor versions. Never copy x86 native libraries into an ARM environment or edit the shared default ARM environment.
- Start with the synthetic Client probe (two one-core Executors); it does not access business S3. Business tests need a fresh, exclusive S3 output prefix or an empty shared filesystem directory, never the existing production output.
- Spark filesystem output accepts an absolute path or `file:///absolute/path`. Verify that Driver and Executors see the same shared directory; separate Pods' `/tmp` directories cannot be used as shared output. Preserve the exclusive run marker during work, atomically publish immutable files, verify hashes, and publish the manifest last. Retain failed-run files for diagnosis; do not reuse a non-empty output directory or recursively clean it automatically.
- Select complete small user prefixes for business smoke tests; do not truncate a user scope by object count. `spark/local_output_smoke.py` reads S3 input but forbids creating an S3 output client. Local unit/integration tests and synthetic Kubernetes probes are not evidence of successful real-S3 business acceptance.
- Explicit `--input-credentials-file` is only a path, loaded inside Executors. The protected file must be owned by the Executor's effective user and have mode `0400` or `0600`; verify parent-directory traversal, matching runtime identities, and shared-output write permissions. Never weaken permissions on another deployment's credentials, copy secrets into Spark configuration, or assume Hadoop S3A credentials are available to boto3. Without an explicit path, retain boto3's default credential chain.
- Keep Kubernetes TLS validation enabled. Read credentials only for authentication; never dump complete Spark configuration or credential-bearing files.
- In the current DolphinScheduler deployment, use the verified Worker-local `/tmp` for S3 normalization state by passing `workspace_parent=Path("/tmp")`. This supersedes the older shared-workspace guidance in `docs/dolphinscheduler.md`; do not place SQLite WAL state under `/share`.
- Keep credentials outside the repository. Never print, inspect in task logs, commit, or push credential values.

## Verification and Git workflow

- Preserve unrelated user changes and inspect `git status` before staging files.
- Run `.venv/bin/python -m pytest -q` before pushing Python changes.
- Do not push `main` from this Spark checkout.
- Review incident and deployment documents for internal hostnames, task identifiers, storage paths, personal paths, and secrets before publishing them to a public repository.
