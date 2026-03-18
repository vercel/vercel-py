import asyncio

from dotenv import load_dotenv

from vercel.sandbox import AsyncSandbox as Sandbox

load_dotenv()


async def main() -> None:
    async with await Sandbox.create(timeout=60_000) as sandbox:
        # Write files and an executable script into the sandbox.
        await sandbox.write_files(
            [
                {"path": "hello.txt", "content": b"hello vercel"},
                {
                    "path": "/vercel/sandbox/nested/dir/note.txt",
                    "content": b"nested file",
                },
                {
                    "path": "hello.sh",
                    "content": b"#!/bin/sh\necho executable hello\n",
                    "mode": 0o755,
                },
            ]
        )

        data1 = await sandbox.read_file("hello.txt")
        data2 = await sandbox.read_file("/vercel/sandbox/nested/dir/note.txt")
        result = await sandbox.run_command("./hello.sh")
        print("hello.txt:", data1.decode())
        print("note.txt:", data2.decode())
        print("hello.sh:", (await result.stdout()).strip())

        # Sleep briefly to keep the sandbox alive for inspection
        await asyncio.sleep(0.1)


if __name__ == "__main__":
    asyncio.run(main())
