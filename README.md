## Lock on S3

- **Purpose**: Distributed mutex implemented with S3. Wrap any long-running job so only one instance runs at a time—perfect for CI/CD, multi-host cron, or blue/green deploys.

- **How It Works**  
  - Creates a lock object at `S3_LOCK_PATH` using `--if-none-match '*'`; only the first writer succeeds.  
  - Leaves a wait trace in `S3_WAIT_PATH`; removes it on exit.  
  - Writes an audit record to `S3_LOCK_ACHIEVED_PATH` when the lock is taken.  
  - Background keep-alive refreshes lock metadata; stale locks are cleaned up via `--if-match`.  
  - Optional fairness: inspect the wait queue and bail out if a newer waiter is detected.

- **Configuration (`lock_on_s3.conf`)**
  - Paths: `BUCKET`, `S3_LOCK_PATH`, `S3_WAIT_PATH`, `S3_LOCK_ACHIEVED_PATH`
  - Timing: `LOCK_STALE_SEC`, `MAX_LOCK_DURATION_SEC`, `LOCK_LOOP_SLEEP_SEC`, `LOCK_VERIFY_DELAY_SEC`, `KEEPALIVE_LOOP_SLEEP_SEC`, `WAIT_STALE_TIME_SEC`
  - Retry: `MAX_RETRIES`, `EXP_BACKOFF_COEF`, `GRACEFUL_SHUTDOWN_TIMEOUT_SEC`

- **Requirements**
  - Bash 5.1+ (uses `wait -f -n -p`)
  - AWS CLI v2, `jq`, `bc`, plus standard Unix utilities (`awk`, `sed`, `mktemp`, etc.)
  - IAM permissions on the configured S3 bucket: `GetObject`, `PutObject`, `DeleteObject`, `HeadObject`

- **Usage**
 
  ./lock_on_s3.sh lock_on_s3.conf "./deploy.sh --env prod" my-task optional-suffix
  - Arg 1: config file  
  - Arg 2: command to run under lock (evaluated by Bash)  
  - Arg 3: task name (appears in wait/lock artifacts)  
  - Arg 4: optional suffix for multi-stage locks

- **Multi-Phase Lock Example**
 
  ./lock_on_s3.sh ./lock_on_s3.conf "./lock_on_s3.sh ./lock_on_s3.conf ./deploy.sh sample 2" sample 1

  - Phase 1 acquires the outer `sample-1` lock, then runs a second lock protected by suffix `sample-2`.  
  - Useful when orchestrating sequential steps that must not overlap (e.g., environment bootstrap followed by migration).

- **Operational Notes**
  - `PROTECTED_CMD` is evaluated after a minimal character filter; intended for trusted operators in CI/CD.  
  - Wait files stay in S3 for audit/debug.  
  - Set `WAIT_STALE_TIME_SEC=0` to skip queue inspection (pure mutex mode).  
  - On keep-alive failure or timeout, the script terminates the guarded process and exits with a dedicated status.

- **Key Exit Codes**
  - `0`: success  
  - `40`: exceeded retry budget acquiring lock  
  - `50+`: cleanup/deletion failures  
  - `60+`: keep-alive errors or intrusion detection  
  - `99`: terminated via signal (SIGINT/SIGTERM)

