import asyncio
import contextlib
import os
import webbrowser

from dotenv import load_dotenv

from vercel.sandbox import AsyncSandbox as Sandbox

load_dotenv()


GIN_APP = b"""
package main

import (
	"log"
	"net"
	"net/http"

	"github.com/gin-gonic/gin"
)

func main() {
	router := gin.Default()

	router.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "Hello from Gin in Vercel Sandbox!",
		})
	})

	port := "3000"

	ln, err := net.Listen("tcp", "0.0.0.0:"+port)
	if err != nil {
		log.Fatalf("listen error: %v", err)
	}
	log.Printf("Listening on http://0.0.0.0:%s", port)

	if err := http.Serve(ln, router); err != nil && err != http.ErrServerClosed {
		log.Fatalf("serve error: %v", err)
	}
}
"""


async def main() -> None:
    runtime = (
        os.getenv("SANDBOX_RUNTIME") or "node22"
    )  # note: go runtime is not pre-installed; install via dnf

    # Gin default port
    port = 3000

    async with await Sandbox.create(ports=[port], timeout=600_000, runtime=runtime) as sandbox:
        # Write the Go Gin application to the sandbox working directory
        await sandbox.write_files(
            [
                {"path": "main.go", "content": GIN_APP},
            ]
        )

        # Ensure Go toolchain is available in the Amazon Linux 2023 base image
        print("Installing Go (golang) and Git via dnf...")
        dnf_cmd = await sandbox.run_command_detached(
            "bash",
            [
                "-lc",
                ("dnf install -y golang git"),
            ],
            sudo=True,
        )
        async for line in dnf_cmd.logs():
            print(line.data, end="")
        dnf_done = await dnf_cmd.wait()
        if dnf_done.exit_code != 0:
            raise SystemExit("dnf install failed")

        print("Initializing Go module and fetching Gin...")
        mod_cmd = await sandbox.run_command_detached(
            "bash",
            [
                "-lc",
                (
                    f"cd {sandbox.sandbox.cwd} && "
                    "go version && "
                    "go mod init ginapp && "
                    "go get github.com/gin-gonic/gin@latest"
                ),
            ],
        )
        async for line in mod_cmd.logs():
            print(line.data, end="")
        mod_done = await mod_cmd.wait()
        if mod_done.exit_code != 0:
            raise SystemExit("go module setup failed")

        print("Starting Gin server...")
        cmd = await sandbox.run_command_detached(
            "bash",
            [
                "-lc",
                (f"cd {sandbox.sandbox.cwd} && go run ."),
            ],
        )

        # Stream logs and open browser once server is ready.
        ready = asyncio.Event()

        async def logs_and_detect_ready():
            async for line in cmd.logs():
                print(line.data, end="")
                if not ready.is_set() and ("Listening on" in line.data and str(port) in line.data):
                    ready.set()

        logs_task = asyncio.create_task(logs_and_detect_ready())
        try:
            await asyncio.wait_for(ready.wait(), timeout=90)
        except asyncio.TimeoutError:
            pass

        url = sandbox.domain(port)
        print("Open:", url)
        # In CI, avoid opening a browser.
        if not os.getenv("CI"):
            with contextlib.suppress(Exception):
                webbrowser.open(url)

        # Stop streaming logs and terminate the server so the example exits promptly.
        logs_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await logs_task
        await cmd.kill()


if __name__ == "__main__":
    asyncio.run(main())
