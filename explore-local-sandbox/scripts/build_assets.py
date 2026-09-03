#!/usr/bin/env python3
"""Build kernel and initramfs assets for native Apple Silicon microVM."""

import gzip
import io
import os
from pathlib import Path
import struct
import subprocess
import sys
import tarfile
import urllib.request

ALPINE_MIRROR = "https://dl-cdn.alpinelinux.org/alpine/v3.20"
ASSETS_DIR = Path(__file__).resolve().parent.parent / "assets"
CACHE_DIR = Path(__file__).resolve().parent.parent / ".cache"

def log(msg: str) -> None:
    print(f"[build-assets] {msg}", flush=True)

def download(url: str, dest: Path) -> None:
    if dest.exists() and dest.stat().st_size > 0:
        log(f"Using cached {dest.name}")
        return
    log(f"Downloading {url} -> {dest}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as resp, open(dest, "wb") as f:
        while chunk := resp.read(65536):
            f.write(chunk)

def unwrap_efi_zboot(vmlinuz_path: Path, vmlinux_path: Path) -> None:
    """Apple's Virtualization.framework requires raw ARM64 Image with 'ARMd' at 0x38."""
    if vmlinux_path.exists() and vmlinux_path.stat().st_size > 0:
        log(f"Using unwrapped {vmlinux_path.name}")
        return
    log(f"Unwrapping EFI zboot {vmlinuz_path.name} -> {vmlinux_path.name}")
    data = vmlinuz_path.read_bytes()
    if data[:2] != b"MZ" or data[4:8] != b"zimg":
        raise RuntimeError("Not a valid EFI zboot image")
    payload_offset, payload_size = struct.unpack("<II", data[8:16])
    comp = data[24:32].split(b"\x00")[0].decode("ascii")
    payload = data[payload_offset : payload_offset + payload_size]
    if comp == "gzip":
        decomp = gzip.decompress(payload)
    else:
        raise RuntimeError(f"Unsupported zboot compression: {comp}")
    if decomp[0x38:0x3C] != b"ARMd":
        raise RuntimeError("Decompressed kernel lacks 'ARMd' ARM64 signature at offset 0x38")
    vmlinux_path.write_bytes(decomp)
    log(f"Kernel ready: {len(decomp)} bytes with valid ARMd signature")

def fetch_apks_for_python(download_dir: Path) -> list[Path]:
    index_url = f"{ALPINE_MIRROR}/main/aarch64/APKINDEX.tar.gz"
    index_tar = download_dir / "APKINDEX.tar.gz"
    download(index_url, index_tar)
    
    with tarfile.open(index_tar, "r:gz") as tar:
        index_txt = tar.extractfile("APKINDEX").read().decode("utf-8", errors="ignore")

    needed_prefixes = [
        "python3-3.",
        "libffi-",
        "gdbm-",
        "bzip2-",
        "xz-libs-",
        "sqlite-libs-",
        "readline-",
        "ncurses-libs-",
        "ncurses-terminfo-base-",
        "mpdecimal-",
        "expat-",
        "libcrypto3-",
        "libssl3-",
        "zlib-",
    ]
    apk_map: dict[str, str] = {}
    current: dict[str, str] = {}
    for line in index_txt.splitlines():
        if line.startswith("P:"):
            current["P"] = line[2:]
        elif line.startswith("V:"):
            current["V"] = line[2:]
        elif line == "":
            pkg = current.get("P", "")
            ver = current.get("V", "")
            full = f"{pkg}-{ver}.apk"
            for prefix in needed_prefixes:
                if full.startswith(prefix) and not pkg.endswith("-dev") and not pkg.endswith("-doc") and not pkg.endswith("-static"):
                    apk_map[pkg] = full
            current = {}

    downloaded = []
    for pkg, filename in apk_map.items():
        dest = download_dir / filename
        download(f"{ALPINE_MIRROR}/main/aarch64/{filename}", dest)
        downloaded.append(dest)
    return downloaded

def extract_kernel_modules(modloop_path: Path, output_modules_dir: Path) -> None:
    output_modules_dir.mkdir(parents=True, exist_ok=True)
    needed = [
        "modules/*/kernel/net/vmw_vsock/*",
        "modules/*/kernel/drivers/net/virtio_net.ko",
        "modules/*/kernel/drivers/net/net_failover.ko",
        "modules/*/kernel/net/core/failover.ko",
        "modules/*/kernel/drivers/char/hw_random/rng-core.ko",
        "modules/*/kernel/drivers/char/hw_random/virtio-rng.ko",
    ]
    # Check if already extracted
    if list(output_modules_dir.glob("**/*.ko")):
        log("Kernel modules already extracted")
        return
    log("Extracting virtio and vsock kernel modules from modloop...")
    tmp_out = output_modules_dir.parent / "_unsquash_tmp"
    subprocess.run(["rm", "-rf", str(tmp_out)], check=True)
    cmd = ["unsquashfs", "-d", str(tmp_out), str(modloop_path)] + needed
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)
    
    # Move extracted modules
    modules_sub = [p for p in tmp_out.glob("modules/*") if p.is_dir() and p.name != "firmware"]
    for kver_dir in modules_sub:
        dest_kver = output_modules_dir / kver_dir.name
        dest_kver.mkdir(parents=True, exist_ok=True)
        subprocess.run(f"cp -r '{kver_dir}/'* '{dest_kver}/'", shell=True, check=True)
    subprocess.run(["rm", "-rf", str(tmp_out)], check=True)
    log("Kernel modules extracted successfully")

def build_initramfs(
    minirootfs_tar: Path,
    apk_files: list[Path],
    modules_dir: Path,
    guest_agent_src: Path,
    output_initramfs: Path,
) -> None:
    log("Building rootfs and initramfs...")
    staging = CACHE_DIR / "rootfs_staging"
    subprocess.run(["rm", "-rf", str(staging)], check=True)
    staging.mkdir(parents=True, exist_ok=True)

    log("Unpacking Alpine minirootfs...")
    subprocess.run(["tar", "-xzf", str(minirootfs_tar), "-C", str(staging)], check=True)

    log("Extracting Python runtime packages...")
    for apk in apk_files:
        subprocess.run(["tar", "-xzf", str(apk), "-C", str(staging)], check=True)
    
    # Remove apk metadata
    for f in staging.glob(".PKGINFO"):
        f.unlink()
    for f in staging.glob(".SIGN.*"):
        f.unlink()

    # Copy kernel modules
    staging_modules = staging / "lib" / "modules"
    staging_modules.mkdir(parents=True, exist_ok=True)
    subprocess.run(f"cp -r '{modules_dir}/'* '{staging_modules}/'", shell=True, check=True)

    # Copy guest agent
    dest_agent = staging / "guest_agent.py"
    dest_agent.write_bytes(guest_agent_src.read_bytes())
    dest_agent.chmod(0o755)

    # Create /init
    init_script = staging / "init"
    init_content = """#!/bin/sh
mount -t devtmpfs dev /dev 2>/dev/null || true
mount -t proc proc /proc 2>/dev/null || true
mount -t sysfs sys /sys 2>/dev/null || true

exec > /dev/hvc0 2>&1

KVER=$(uname -r)
insmod /lib/modules/$KVER/kernel/drivers/char/hw_random/rng-core.ko 2>/dev/null || true
insmod /lib/modules/$KVER/kernel/drivers/char/hw_random/virtio-rng.ko 2>/dev/null || true
insmod /lib/modules/$KVER/kernel/net/vmw_vsock/vsock.ko 2>/dev/null || true
insmod /lib/modules/$KVER/kernel/net/vmw_vsock/vmw_vsock_virtio_transport_common.ko 2>/dev/null || true
insmod /lib/modules/$KVER/kernel/net/vmw_vsock/vmw_vsock_virtio_transport.ko 2>/dev/null || true

echo "[guest-init] Kernel booted, starting guest agent..."
/usr/bin/python3 /guest_agent.py &

exec /bin/sh
"""
    init_script.write_text(init_content)
    init_script.chmod(0o755)

    # Pack cpio.gz
    log(f"Creating initramfs archive -> {output_initramfs}")
    output_initramfs.parent.mkdir(parents=True, exist_ok=True)
    find_proc = subprocess.Popen(["find", "."], cwd=staging, stdout=subprocess.PIPE)
    cpio_proc = subprocess.Popen(["cpio", "-o", "-H", "newc"], cwd=staging, stdin=find_proc.stdout, stdout=subprocess.PIPE)
    find_proc.stdout.close()
    with open(output_initramfs, "wb") as out_f:
        gzip_proc = subprocess.Popen(["gzip", "-1"], stdin=cpio_proc.stdout, stdout=out_f)
        cpio_proc.stdout.close()
        gzip_proc.communicate()
    log(f"Initramfs generated: {output_initramfs.stat().st_size / (1024*1024):.2f} MB")

def main() -> None:
    ASSETS_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    vmlinuz_path = CACHE_DIR / "vmlinuz-virt"
    vmlinux_path = ASSETS_DIR / "vmlinux"
    initramfs_path = ASSETS_DIR / "initramfs.cpio.gz"
    modloop_path = CACHE_DIR / "modloop-virt"
    minirootfs_path = CACHE_DIR / "minirootfs.tar.gz"

    # 1. Download Alpine netboot kernel & modloop
    download(f"{ALPINE_MIRROR}/releases/aarch64/netboot/vmlinuz-virt", vmlinuz_path)
    download(f"{ALPINE_MIRROR}/releases/aarch64/netboot/modloop-virt", modloop_path)
    download(f"{ALPINE_MIRROR}/releases/aarch64/alpine-minirootfs-3.20.3-aarch64.tar.gz", minirootfs_path)

    # 2. Decompress kernel to raw ARM64 vmlinux
    unwrap_efi_zboot(vmlinuz_path, vmlinux_path)

    # 3. Fetch python runtime packages
    apks_dir = CACHE_DIR / "apks"
    apk_files = fetch_apks_for_python(apks_dir)

    # 4. Extract kernel modules
    modules_dir = CACHE_DIR / "modules"
    extract_kernel_modules(modloop_path, modules_dir)

    # 5. Build initramfs
    guest_agent = Path(__file__).resolve().parent.parent / "src" / "guest" / "agent.py"
    build_initramfs(minirootfs_path, apk_files, modules_dir, guest_agent, initramfs_path)
    log("All microVM assets ready in explore-local-sandbox/assets/")

if __name__ == "__main__":
    main()
