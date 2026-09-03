# Local Apple Silicon MicroVM Sandbox Exploration

This directory contains a prototype of **local sandboxes** running lightweight Linux microVMs on macOS via Apple's native **Virtualization.framework** (using `vfkit`), inspired by [Encore's microVM architecture](https://encore.dev/blog/firecracker-apple-silicon).

## Architecture

```
                                          macOS Host
┌─────────────────────────────────────────────────────────────────────────────┐
│  vercel.sandbox SDK / Examples                                              │
│         │                                                                   │
│         ▼ HTTP (REST /v3/sandboxes, /cmd, /fs)                              │
│  Local Sandbox Daemon (explore-local-sandbox/src/host/daemon.py)             │
│         │                                                                   │
│         ▼ Unix Domain Socket Bridge (AF_UNIX)                               │
│  vfkit (Apple Virtualization.framework hypervisor)                         │
└─────────┼───────────────────────────────────────────────────────────────────┘
          │ virtio-vsock (port 10000)
┌─────────┼───────────────────────────────────────────────────────────────────┐
│         ▼                                                                   │
│  Guest Agent (explore-local-sandbox/src/guest/agent.py)                     │
│  Linux Kernel (raw uncompressed ARM64 vmlinux)                              │
│  Alpine Rootfs + Python 3 Runtime                                           │
│                                                                             │
│                                           Native Apple Silicon MicroVM      │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Structure

- `scripts/build_assets.py`: Downloads Alpine Linux aarch64 kernel & packages, unwraps the EFI zboot header to produce raw uncompressed `vmlinux`, extracts the virtio/vsock drivers, and bundles the guest agent and Python 3.12 runtime into `assets/initramfs.cpio.gz`.
- `src/guest/agent.py`: Guest agent executed inside the microVM that listens on `AF_VSOCK` port 10000 and executes processes, reads/writes files, and manages directories.
- `src/host/daemon.py`: Host-side daemon and Vercel Sandbox API server translating SDK requests into microVM operations over vsock.
- `run_local_example.py`: Executes real `vercel-sandbox` examples against the local microVM.
