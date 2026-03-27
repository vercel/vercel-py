from __future__ import annotations

from collections.abc import AsyncGenerator, Generator
from dataclasses import dataclass

from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.sandbox import (
    AsyncSandboxOpsClient,
    SandboxNotFoundError,
    SyncSandboxOpsClient,
)
from vercel._internal.sandbox.models import (
    Command as CommandModel,
    CommandFinishedResponse,
    LogLine,
)


@dataclass
class AsyncCommand:
    client: AsyncSandboxOpsClient
    sandbox_id: str
    cmd: CommandModel

    @property
    def cmd_id(self) -> str:
        return self.cmd.id

    @property
    def cwd(self) -> str:
        return self.cmd.cwd

    @property
    def started_at(self) -> int:
        return self.cmd.started_at

    async def logs(self) -> AsyncGenerator[LogLine, None]:
        async for log in self.client.get_logs(sandbox_id=self.sandbox_id, cmd_id=self.cmd.id):
            yield log

    async def wait(self) -> AsyncCommandFinished:
        resp = await self.client.get_command(
            sandbox_id=self.sandbox_id, cmd_id=self.cmd.id, wait=True
        )
        assert isinstance(resp, CommandFinishedResponse)
        return AsyncCommandFinished(
            client=self.client,
            sandbox_id=self.sandbox_id,
            cmd=resp.command,
            exit_code=resp.command.exit_code,
        )

    async def output(self, stream: str = "both") -> str:
        data = ""
        async for log in self.logs():
            if stream == "both" or log.stream == stream:
                data += log.data
        return data

    async def stdout(self) -> str:
        return await self.output("stdout")

    async def stderr(self) -> str:
        return await self.output("stderr")

    async def kill(self, signal: int = 15) -> None:
        try:
            await self.client.kill_command(
                sandbox_id=self.sandbox_id, command_id=self.cmd.id, signal=signal
            )
        except SandboxNotFoundError:
            # Command may already have exited; ignore 404s
            return


@dataclass
class AsyncCommandFinished(AsyncCommand):
    exit_code: int

    async def wait(self) -> AsyncCommandFinished:
        return self


# Sync command API


@dataclass
class Command:
    client: SyncSandboxOpsClient
    sandbox_id: str
    cmd: CommandModel

    @property
    def cmd_id(self) -> str:
        return self.cmd.id

    @property
    def cwd(self) -> str:
        return self.cmd.cwd

    @property
    def started_at(self) -> int:
        return self.cmd.started_at

    def logs(self) -> Generator[LogLine, None, None]:
        yield from self.client.get_logs(sandbox_id=self.sandbox_id, cmd_id=self.cmd.id)

    def wait(self) -> CommandFinished:
        resp = iter_coroutine(
            self.client.get_command(sandbox_id=self.sandbox_id, cmd_id=self.cmd.id, wait=True)
        )
        assert isinstance(resp, CommandFinishedResponse)
        return CommandFinished(
            client=self.client,
            sandbox_id=self.sandbox_id,
            cmd=resp.command,
            exit_code=resp.command.exit_code,
        )

    def output(self, stream: str = "both") -> str:
        data = ""
        for log in self.logs():
            if stream == "both" or log.stream == stream:
                data += log.data
        return data

    def stdout(self) -> str:
        return self.output("stdout")

    def stderr(self) -> str:
        return self.output("stderr")

    def kill(self, signal: int = 15) -> None:
        try:
            iter_coroutine(
                self.client.kill_command(
                    sandbox_id=self.sandbox_id, command_id=self.cmd.id, signal=signal
                )
            )
        except SandboxNotFoundError:
            return


@dataclass
class CommandFinished(Command):
    exit_code: int

    def wait(self) -> CommandFinished:
        return self
