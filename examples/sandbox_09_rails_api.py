import asyncio
import contextlib
import os
import webbrowser
from urllib.parse import urlparse

from dotenv import load_dotenv

from vercel.sandbox import AsyncSandbox as Sandbox

load_dotenv()


ROUTES_SCAFFOLD_RB = b"""
Rails.application.routes.draw do
  resources :posts
  root "posts#index"
end
"""


HOSTS_INITIALIZER = b"""
# Allow sandbox host passed via environment variable
if ENV['ALLOWED_HOST'] && !ENV['ALLOWED_HOST'].empty?
  Rails.application.config.hosts << ENV['ALLOWED_HOST']
end
"""

SEEDS_RB = b"""
# Seed some sample posts for the demo
Post.destroy_all
Post.create!([
  { title: "Hello", body: "First post from Rails on Vercel Sandbox." },
  { title: "Rails + Sandbox", body: "It works! CRUD ready." }
])
puts "Seeded #{Post.count} posts"
"""


async def main() -> None:
    runtime = (
        os.getenv("SANDBOX_RUNTIME") or "node22"
    )  # note: ruby runtime is not supported so we install ruby via dnf

    # Rails default port is 3000
    port = 3000
    app_name = "rails_api"

    async with await Sandbox.create(ports=[port], timeout=600_000, runtime=runtime) as sandbox:
        # Ensure Ruby 3.2 and build tools are available in the Amazon Linux 2023 base image
        print("Installing Ruby 3.2, SQLite, and build tools via dnf...")
        dnf_cmd = await sandbox.run_command_detached(
            "bash",
            [
                "-lc",
                (
                    "dnf install -y "
                    "ruby3.2 ruby3.2-rubygems ruby3.2-rubygem-json ruby3.2-devel "
                    "libyaml-devel "
                    "gcc gcc-c++ make git sqlite sqlite-devel"
                ),
            ],
            sudo=True,
        )
        async for line in dnf_cmd.logs():
            print(line.data, end="")
        dnf_done = await dnf_cmd.wait()
        if dnf_done.exit_code != 0:
            raise SystemExit("dnf install failed")

        # Install Bundler and Rails globally (requires sudo)
        print("Installing Bundler and Rails gems...")
        gem_install_cmd = await sandbox.run_command_detached(
            "bash",
            [
                "-lc",
                ("gem install --no-document bundler && gem install --no-document rails"),
            ],
            sudo=True,
        )
        async for line in gem_install_cmd.logs():
            print(line.data, end="")
        gem_install_done = await gem_install_cmd.wait()
        if gem_install_done.exit_code != 0:
            raise SystemExit("gem install rails/bundler failed")

        # Create a new (non API-only) Rails app
        print("Generating new Rails app...")
        new_cmd = await sandbox.run_command_detached(
            "bash",
            [
                "-lc",
                (
                    f"cd {sandbox.sandbox.cwd} && "
                    f"rails new {app_name} "
                    "--database=sqlite3 "
                    "--skip-asset-pipeline "
                    "--skip-javascript "
                    "--skip-hotwire "
                    "--skip-jbuilder "
                    "--skip-action-mailbox "
                    "--skip-action-text "
                    "--skip-active-storage "
                    "--skip-action-cable "
                    "--skip-system-test "
                    "--skip-git "
                    "--force"
                ),
            ],
        )
        async for line in new_cmd.logs():
            print(line.data, end="")
        new_done = await new_cmd.wait()
        if new_done.exit_code != 0:
            raise SystemExit("rails new failed")

        app_path = f"{sandbox.sandbox.cwd}/{app_name}"

        # Configure bundler to install into a writable path inside the app directory
        print("Configuring Bundler to use vendor/bundle...")
        bundler_cfg_cmd = await sandbox.run_command_detached(
            "bash",
            [
                "-lc",
                (f"cd {app_path} && bundle config set --local path vendor/bundle"),
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
                (f"cd {app_path} && bundle install --jobs 4 --retry 3"),
            ],
        )
        async for line in bundle_install_cmd.logs():
            print(line.data, end="")
        bundle_install_done = await bundle_install_cmd.wait()
        if bundle_install_done.exit_code != 0:
            raise SystemExit("bundle install failed")

        # Generate a CRUD scaffold for Post (title:string, body:text)
        print("Generating scaffold for Post...")
        scaffold_cmd = await sandbox.run_command_detached(
            "bash",
            [
                "-lc",
                (
                    f"cd {app_path} && "
                    "bundle exec rails generate scaffold Post title:string body:text"
                ),
            ],
        )
        async for line in scaffold_cmd.logs():
            print(line.data, end="")
        scaffold_done = await scaffold_cmd.wait()
        if scaffold_done.exit_code != 0:
            raise SystemExit("rails generate scaffold failed")

        # Overwrite routes to ensure resources and root
        print("Configuring routes and seeds...")
        await sandbox.write_files(
            [
                {"path": f"{app_name}/config/routes.rb", "content": ROUTES_SCAFFOLD_RB},
                {"path": f"{app_name}/db/seeds.rb", "content": SEEDS_RB},
                {
                    "path": f"{app_name}/config/initializers/allow_hosts.rb",
                    "content": HOSTS_INITIALIZER,
                },
            ]
        )

        # Run database migrations
        print("Running migrations...")
        migrate_cmd = await sandbox.run_command_detached(
            "bash",
            [
                "-lc",
                (f"cd {app_path} && bundle exec rails db:migrate"),
            ],
        )
        async for line in migrate_cmd.logs():
            print(line.data, end="")
        migrate_done = await migrate_cmd.wait()
        if migrate_done.exit_code != 0:
            raise SystemExit("rails db:migrate failed")

        # Seed sample data
        print("Seeding database...")
        seed_cmd = await sandbox.run_command_detached(
            "bash",
            [
                "-lc",
                (f"cd {app_path} && bundle exec rails db:seed"),
            ],
        )
        async for line in seed_cmd.logs():
            print(line.data, end="")
        seed_done = await seed_cmd.wait()
        if seed_done.exit_code != 0:
            raise SystemExit("rails db:seed failed")

        # Start Rails server (development env) binding to 0.0.0.0
        print("Starting Rails server...")
        # Determine sandbox hostname for host authorization
        sandbox_url = sandbox.domain(port)
        allowed_host = urlparse(sandbox_url).hostname or ""
        cmd = await sandbox.run_command_detached(
            "bash",
            [
                "-lc",
                (
                    f"cd {app_path} && "
                    f"ALLOWED_HOST={allowed_host} bundle exec rails server -b 0.0.0.0 -p {port}"
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
                    and f":{port}" in line.data
                    or "Use Ctrl-C to stop" in line.data
                    or "Puma starting" in line.data
                ):
                    ready.set()

        logs_task = asyncio.create_task(logs_and_detect_ready())
        try:
            await asyncio.wait_for(ready.wait(), timeout=120)
        except asyncio.TimeoutError:
            pass

        url = sandbox_url
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
