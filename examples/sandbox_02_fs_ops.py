import asyncio

from dotenv import load_dotenv

from vercel.sandbox import AsyncSandbox as Sandbox

load_dotenv()


async def main() -> None:
    async with await Sandbox.create(timeout=60_000) as sandbox:
        # Write file into /vercel/sandbox/hello.txt
        await sandbox.write_files(
            [
                {"path": "hello.txt", "content": b"hello vercel"},
                {
                    "path": "/vercel/sandbox/nested/dir/note.txt",
                    "content": b"nested file",
                },
            ]
        )

        data1 = await sandbox.read_file("hello.txt")
        data2 = await sandbox.read_file("/vercel/sandbox/nested/dir/note.txt")
        print("hello.txt:", data1.decode())
        print("note.txt:", data2.decode())

        # Sleep briefly to keep the sandbox alive for inspection
        await asyncio.sleep(0.1)


if __name__ == "__main__":
    asyncio.run(main())
