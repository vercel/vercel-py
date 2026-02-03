import asyncio

from dotenv import load_dotenv

from vercel.sandbox import AsyncSandbox as Sandbox

load_dotenv()


async def main() -> None:
    # Requires env vars: VERCEL_OIDC_TOKEN + VERCEL_PROJECT_ID + VERCEL_TEAM_ID
    async with await Sandbox.create(timeout=60_000) as sandbox:  # 1 minute sandbox
        # Simple echo
        cmd = await sandbox.run_command("bash", ["-lc", "echo hello && echo world 1>&2"])
        print("exit:", cmd.exit_code)

        # Detached + logs streaming (Ctrl+C will trigger cleanup via context manager)
        detached = await sandbox.run_command_detached(
            "bash", ["-lc", "for i in 1 2 3; do echo $i; sleep 0.2; done"]
        )
        async for line in detached.logs():
            print(line.stream, line.data, end="")
        await detached.wait()


if __name__ == "__main__":
    asyncio.run(main())
