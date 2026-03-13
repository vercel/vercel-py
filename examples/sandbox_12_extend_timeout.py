"""
Example: Extend Timeout (Sync & Async)

This example demonstrates how to:
1. Create a sandbox with a specific timeout
2. Check the current timeout value
3. Extend the timeout duration
4. Verify the timeout was extended

The extend_timeout method allows you to extend the lifetime of a running sandbox
up until the maximum execution timeout for your plan.
"""

import asyncio

from dotenv import load_dotenv

from vercel.sandbox import AsyncSandbox, Sandbox

load_dotenv()


async def async_demo() -> None:
    print("=" * 60)
    print("ASYNC EXTEND TIMEOUT EXAMPLE")
    print("=" * 60)

    # Step 1: Create a sandbox with a 2-minute timeout
    initial_timeout = 2 * 60 * 1000  # 2 minutes in milliseconds
    print(f"\n[1] Creating sandbox with {initial_timeout // 1000}s timeout...")
    sandbox = await AsyncSandbox.create(timeout=initial_timeout)
    try:
        print(f"    Sandbox ID: {sandbox.sandbox_id}")
        print(f"    Initial timeout: {sandbox.timeout}ms ({sandbox.timeout // 1000}s)")

        # Step 2: Extend the timeout by 3 minutes
        extension = 3 * 60 * 1000  # 3 minutes in milliseconds
        print(f"\n[2] Extending timeout by {extension // 1000}s...")
        await sandbox.extend_timeout(extension)
        print(f"    New timeout: {sandbox.timeout}ms ({sandbox.timeout // 1000}s)")

        # Verify the timeout was extended
        expected_timeout = initial_timeout + extension
        assert sandbox.timeout == expected_timeout, (
            f"Expected timeout {expected_timeout}, got {sandbox.timeout}"
        )
        print("\n✓ Async extend_timeout test passed!")

    finally:
        await sandbox.stop()
        await sandbox.client.aclose()


def sync_demo() -> None:
    print("\n" + "=" * 60)
    print("SYNC EXTEND TIMEOUT EXAMPLE")
    print("=" * 60)

    # Step 1: Create a sandbox with a 2-minute timeout
    initial_timeout = 2 * 60 * 1000  # 2 minutes in milliseconds
    print(f"\n[1] Creating sandbox with {initial_timeout // 1000}s timeout...")
    sandbox = Sandbox.create(timeout=initial_timeout)
    try:
        print(f"    Sandbox ID: {sandbox.sandbox_id}")
        print(f"    Initial timeout: {sandbox.timeout}ms ({sandbox.timeout // 1000}s)")

        # Step 2: Extend the timeout by 3 minutes
        extension = 3 * 60 * 1000  # 3 minutes in milliseconds
        print(f"\n[2] Extending timeout by {extension // 1000}s...")
        sandbox.extend_timeout(extension)
        print(f"    New timeout: {sandbox.timeout}ms ({sandbox.timeout // 1000}s)")

        # Verify the timeout was extended
        expected_timeout = initial_timeout + extension
        assert sandbox.timeout == expected_timeout, (
            f"Expected timeout {expected_timeout}, got {sandbox.timeout}"
        )
        print("\n✓ Sync extend_timeout test passed!")

    finally:
        sandbox.stop()
        sandbox.client.close()


if __name__ == "__main__":
    asyncio.run(async_demo())
    sync_demo()
    print("\n" + "=" * 60)
    print("ALL EXTEND TIMEOUT TESTS COMPLETE")
    print("=" * 60)
