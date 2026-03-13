#!/usr/bin/env python3
"""Example: PTY Infrastructure Test

This example tests the PTY infrastructure without taking over the terminal.
Useful for debugging and verifying the setup works correctly.

Usage:
    python examples/15_pty_test.py
"""

import asyncio

from dotenv import load_dotenv

from vercel.sandbox import AsyncSandbox
from vercel.sandbox.pty.binary import (
    BINARY_BASE_URL,
    CACHE_DIR,
    DEFAULT_SANDBOX_ARCH,
    SERVER_BIN_NAME,
    download_binary_async,
    get_binary_cache_path,
)

load_dotenv()


async def test_binary_download():
    """Test downloading the PTY server binary."""
    print("=" * 60)
    print("Testing PTY Server Binary Download")
    print("=" * 60)
    print()

    print(f"Binary URL: {BINARY_BASE_URL}")
    print(f"Cache directory: {CACHE_DIR}")
    print(f"Server binary name: {SERVER_BIN_NAME}")
    print(f"Default architecture: {DEFAULT_SANDBOX_ARCH}")
    print()

    cache_path = get_binary_cache_path()
    print(f"Expected cache path: {cache_path}")
    print(f"Binary cached: {cache_path.exists()}")
    print()

    print("Downloading binary (will use cache if available)...")
    path = await download_binary_async()
    print(f"Binary path: {path}")
    print(f"Binary size: {path.stat().st_size:,} bytes")
    print()
    print("✅ Binary download test passed!")
    return True


async def test_sandbox_interactive_creation():
    """Test creating a sandbox with interactive support."""
    print()
    print("=" * 60)
    print("Testing Sandbox Creation with Interactive Support")
    print("=" * 60)
    print()

    print("Creating sandbox with interactive=True...")
    sandbox = await AsyncSandbox.create(
        interactive=True,
        timeout=60_000,  # 1 minute
    )

    print(f"Sandbox ID: {sandbox.sandbox_id}")
    print(f"Status: {sandbox.status}")
    print(f"Interactive port: {sandbox.interactive_port}")
    print()

    if sandbox.interactive_port:
        print("✅ Interactive port allocated!")
    else:
        print("❌ No interactive port - API may not support __interactive flag")

    # Test that the sandbox works
    print()
    print("Testing basic command execution...")
    result = await sandbox.run_command("echo", ["Hello from sandbox!"])
    output = await result.stdout()
    print(f"Output: {output.strip()}")
    print(f"Exit code: {result.exit_code}")

    # Get architecture
    print()
    print("Checking sandbox architecture...")
    result = await sandbox.run_command("uname", ["-m"])
    arch = (await result.stdout()).strip()
    print(f"Sandbox architecture: {arch}")

    # Stop sandbox
    print()
    print("Stopping sandbox...")
    await sandbox.stop()
    print("✅ Sandbox test passed!")

    return sandbox.interactive_port is not None


async def test_pty_server_upload():
    """Test uploading and running the PTY server in a sandbox."""
    print()
    print("=" * 60)
    print("Testing PTY Server Upload and Execution")
    print("=" * 60)
    print()

    from vercel.sandbox.pty.binary import get_binary_bytes_async
    from vercel.sandbox.pty.shell import SERVER_BIN_NAME

    print("Creating sandbox...")
    sandbox = await AsyncSandbox.create(
        interactive=True,
        timeout=120_000,  # 2 minutes
    )

    try:
        print(f"Sandbox ID: {sandbox.sandbox_id}")
        print()

        # Download binary
        print("Downloading PTY server binary...")
        binary = await get_binary_bytes_async()
        print(f"Binary size: {len(binary):,} bytes")

        # Upload to sandbox
        print()
        print("Uploading binary to sandbox...")
        tmp_path = f"/tmp/{SERVER_BIN_NAME}-test"
        await sandbox.write_files([{"path": tmp_path, "content": binary}])

        # Make executable and move
        print("Installing binary...")
        result = await sandbox.run_command(
            "bash",
            [
                "-c",
                f'mv "{tmp_path}" /usr/local/bin/{SERVER_BIN_NAME} && '
                f"chmod +x /usr/local/bin/{SERVER_BIN_NAME}",
            ],
            sudo=True,
        )

        if result.exit_code != 0:
            print(f"❌ Failed to install binary: {await result.stderr()}")
            return False

        # Verify installation
        print()
        print("Verifying installation...")
        result = await sandbox.run_command("which", [SERVER_BIN_NAME])
        if result.exit_code == 0:
            path = (await result.stdout()).strip()
            print(f"Binary installed at: {path}")
        else:
            print("❌ Binary not found in PATH")
            return False

        # Check version/help
        print()
        print("Checking binary help...")
        result = await sandbox.run_command(SERVER_BIN_NAME, ["--help"])
        output = await result.stdout()
        stderr = await result.stderr()

        # The help might go to stderr
        help_text = output or stderr
        if "PTY Tunnel Server" in help_text or "WebSocket" in help_text:
            print("✅ Binary runs correctly!")
            print()
            print("First few lines of help output:")
            for line in help_text.split("\n")[:5]:
                print(f"  {line}")
        else:
            print("Binary output:")
            print(help_text[:500])

        print()
        print("✅ PTY server upload test passed!")
        return True

    finally:
        print()
        print("Stopping sandbox...")
        await sandbox.stop()


async def main():
    """Run all PTY infrastructure tests."""
    print()
    print("🧪 PTY Infrastructure Test Suite")
    print()

    results = []

    # Test 1: Binary download
    try:
        results.append(("Binary Download", await test_binary_download()))
    except Exception as e:
        print(f"❌ Binary download failed: {e}")
        results.append(("Binary Download", False))

    # Test 2: Sandbox creation with interactive
    try:
        results.append(("Sandbox Interactive Creation", await test_sandbox_interactive_creation()))
    except Exception as e:
        print(f"❌ Sandbox creation failed: {e}")
        results.append(("Sandbox Interactive Creation", False))

    # Test 3: PTY server upload
    try:
        results.append(("PTY Server Upload", await test_pty_server_upload()))
    except Exception as e:
        print(f"❌ PTY server upload failed: {e}")
        results.append(("PTY Server Upload", False))

    # Summary
    print()
    print("=" * 60)
    print("Test Summary")
    print("=" * 60)
    for name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {status}: {name}")

    all_passed = all(passed for _, passed in results)
    print()
    if all_passed:
        print("🎉 All tests passed!")
    else:
        print("⚠️  Some tests failed")

    return all_passed


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)
