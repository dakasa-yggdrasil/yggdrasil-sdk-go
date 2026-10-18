# Source-owned executor worker (default off)

This additive library has no startup registration, credentials or listener. The
operator constructs a dedicated root-owned Linux cgroup v2 subtree, a root-owned
0700 durable state directory, an exact adopted EC2 identity and fixed command
profiles. Call NewLinuxKernel, New(Config{Enabled:true,...}, kernel), then
worker.Register(adapter). Run the adapter with the ordinary authenticated RPC
transport. Never expose a worker transport to arbitrary callers.

Wire DTOs are in protocol.go. The existing execute envelope carries operation
and input. Operations are observe_executor_worker,
ensure_executor_admission_closed, ensure_executor_admission_open and
ensure_executor_job. No workflow may assert Complete or Ready. Inputs have no
argv, environment or filesystem path. Epochs are positive and admission changes
require a strictly newer epoch. Job retries use the same job ID/profile and do
not spawn again, including after a lost reply. Profile commands are operator
configuration; they must neither invoke privileged services nor access Docker,
containerd or other out-of-cgroup launch sockets. This source covers ordinary
Linux process descendants in its cgroup subtree only. It does not claim Docker
or container jobs outside that subtree are inventoried.

Linux creation uses exec.Cmd UseCgroupFD (CLONE_INTO_CGROUP before exec), drops
UID/GID and all supplementary groups and runs the fixed /usr/bin/setpriv
--no-new-privs wrapper. Both executable paths/ancestors must be root owned and
not group/world writable or symlinks. Install util-linux/setpriv on the target.
A dedicated job UID must have no privileged service/socket/device access.
The cgroup root and all ancestors must be root-owned and not writable by jobs.
Do not delegate child cgroup control to the job UID. This is process accounting,
not a general-purpose hostile code sandbox or a defense against a privileged
operator. Configure filesystem/socket/device isolation separately before using
untrusted test code. No such access configuration is performed by this library.

Before sending, a bounded journal is fsynced, atomically renamed and its directory
fsynced. The original process Wait goroutine survives RPC and AI lease expiry.
Unknown children, census/identity loss, corrupt or missing lifetime evidence,
uncertain spawn, and restart with unjoined/open activity return Complete=false
and Ready=false (HOLD). HOLD is sticky and never auto-cleared by readback.
The journal retains up to MaxJobs tombstones; exhaustion refuses new work and
requires operator reconciliation/rotation while stopped. No automatic GC or
recovery claims. Worker state and cgroup roots must not be reset to evade HOLD.

Local mocked tests run on macOS without spawning processes. Linux source is
cross-compiled; real cgroup v2, setpriv, ownership, fsync/power-loss and process
qualification remain required on the isolated Linux host before enabling.
