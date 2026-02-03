import asyncio

from dotenv import load_dotenv

from vercel.sandbox import AsyncSandbox, Sandbox

load_dotenv()


async def async_demo() -> None:
    # Requires env vars: VERCEL_OIDC_TOKEN + VERCEL_PROJECT_ID + VERCEL_TEAM_ID
    async with await AsyncSandbox.create(timeout=60_000) as sandbox:  # 1 minute sandbox
        # One-shot command (waits)
        res = await sandbox.run_command("bash", ["-lc", "echo async hello && echo async err 1>&2"])
        print("async exit:", res.exit_code)

        # Detached + logs streaming
        detached = await sandbox.run_command_detached(
            "bash", ["-lc", "for i in 1 2 3; do echo async $i; sleep 0.1; done"]
        )
        async for line in detached.logs():
            print("async", line.stream, line.data, end="")
        await detached.wait()


def sync_demo() -> None:
    # Requires env vars: VERCEL_OIDC_TOKEN + VERCEL_PROJECT_ID + VERCEL_TEAM_ID
    with Sandbox.create(timeout=60_000) as sandbox:  # 1 minute sandbox
        # One-shot command (waits)
        res = sandbox.run_command("bash", ["-lc", "echo sync hello && echo sync err 1>&2"])
        print("sync exit:", res.exit_code)

        # Detached + logs streaming
        detached = sandbox.run_command_detached(
            "bash", ["-lc", "for i in 1 2 3; do echo sync $i; sleep 0.1; done"]
        )
        for line in detached.logs():
            print("sync", line.stream, line.data, end="")
        detached.wait()


if __name__ == "__main__":
    asyncio.run(async_demo())
    sync_demo()
