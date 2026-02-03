import asyncio
import contextlib
import os
import webbrowser

from dotenv import load_dotenv

from vercel.sandbox import AsyncSandbox as Sandbox

load_dotenv()


SINATRA_APP = b"""
require 'json'
require 'sinatra'

set :bind, '0.0.0.0'
set :port, 4567
set :server, :webrick
disable :protection

get '/' do
  content_type :json
  { message: 'Hello from Sinatra in Vercel Sandbox!' }.to_json
end
"""


CONFIG_RU = b"""
# frozen_string_literal: true
ENV['RACK_ENV'] ||= 'production'
require_relative './app'
run Sinatra::Application
"""


GEMFILE = b"""
source 'https://rubygems.org'

gem 'sinatra'
gem 'webrick'
gem 'rackup'
"""


async def main() -> None:
    runtime = (
        os.getenv("SANDBOX_RUNTIME") or "node22"
    )  # note: ruby runtime is not supported so we have to install ruby via dnf

    # Sinatra default port is 4567
    port = 4567

    async with await Sandbox.create(ports=[port], timeout=600_000, runtime=runtime) as sandbox:
        # Write the Sinatra application to the sandbox working directory
        await sandbox.write_files(
            [
                {"path": "app.rb", "content": SINATRA_APP},
                {"path": "config.ru", "content": CONFIG_RU},
                {"path": "Gemfile", "content": GEMFILE},
            ]
        )

        # Ensure Ruby 3.2 and JSON lib are available in the Amazon Linux 2023 base image
        print("Installing Ruby 3.2 and JSON via dnf...")
        apt_cmd = await sandbox.run_command_detached(
            "bash",
            [
                "-lc",
                ("dnf install -y ruby3.2 ruby3.2-rubygems ruby3.2-rubygem-json"),
            ],
            sudo=True,
        )
        async for line in apt_cmd.logs():
            print(line.data, end="")
        apt_done = await apt_cmd.wait()
        if apt_done.exit_code != 0:
            raise SystemExit("dnf install failed")

        # Install Bundler globally (requires sudo); later gem installs will go to vendor/bundle
        print("Installing Bundler...")
        bundler_cmd = await sandbox.run_command_detached(
            "bash",
            [
                "-lc",
                ("gem install --no-document bundler"),
            ],
            sudo=True,
        )
        async for line in bundler_cmd.logs():
            print(line.data, end="")
        bundler_done = await bundler_cmd.wait()
        if bundler_done.exit_code != 0:
            raise SystemExit("Bundler installation failed")

        # Configure bundler to install into a writable path inside the sandbox working directory
        print("Configuring Bundler to use vendor/bundle...")
        bundler_cfg_cmd = await sandbox.run_command_detached(
            "bash",
            [
                "-lc",
                (
                    f"cd {sandbox.sandbox.cwd} && "
                    "bundle config set --local path vendor/bundle && "
                    "bundle config set --local without 'development:test'"
                ),
            ],
        )
        async for line in bundler_cfg_cmd.logs():
            print(line.data, end="")
        bundler_cfg_done = await bundler_cfg_cmd.wait()
        if bundler_cfg_done.exit_code != 0:
            raise SystemExit("Bundler config failed")

        # Install gems from Gemfile into vendor/bundle
        print("Running bundle install...")
        bundle_install_cmd = await sandbox.run_command_detached(
            "bash",
            [
                "-lc",
                (f"cd {sandbox.sandbox.cwd} && bundle install --jobs 4 --retry 3"),
            ],
        )
        async for line in bundle_install_cmd.logs():
            print(line.data, end="")
        bundle_install_done = await bundle_install_cmd.wait()
        if bundle_install_done.exit_code != 0:
            raise SystemExit("bundle install failed")

        print("Starting Sinatra server...")
        cmd = await sandbox.run_command_detached(
            "bash",
            [
                "-lc",
                (
                    f"cd {sandbox.sandbox.cwd} && "
                    # Start via rackup using WEBrick, binding to 0.0.0.0 and selected port
                    f"RACK_ENV=production bundle exec rackup -s webrick -o 0.0.0.0 -p {port}"
                ),
            ],
        )

        # Stream logs and open browser once server is ready.
        ready = asyncio.Event()

        async def logs_and_detect_ready():
            async for line in cmd.logs():
                print(line.data, end="")
                if not ready.is_set() and (
                    "Listening on" in line.data
                    or "WEBrick::HTTPServer#start" in line.data
                    or "Sinatra has taken the stage" in line.data
                    or f"tcp://0.0.0.0:{port}" in line.data
                    or "WEBrick::HTTPServer#start: pid=" in line.data
                ):
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
