# Tooling

Repository tooling for checks, changelogs, releases, and publishing, all driven from the workspace root through Poe tasks.

Contributor commands are listed in [[CONTRIBUTING.md]]; this page explains the design behind them.

## Workspace tasks

Lint, typecheck, and test tasks are defined once and inherited by every package, then fanned out by a Python runner.

Packages include the shared [[scripts/poe/poe.toml]] and only override tasks for genuine differences. The runner in [[scripts/poe/workspace_poe.py]] maps scopes — package names, `root`, or paths — to owning packages and runs them in parallel; the full design is in [[scripts/poe/README.md]].

Example tests are opt-in: a package declares its own `test-examples` task and owns discovery, credentials, setup, and cleanup, so live examples skip safely when secrets are missing.

### Git hooks

Repo-managed hooks run the same checks against exactly what is being committed or pushed, not the dirty worktree.

`uv run poe setup` installs hooks from `scripts/githooks/`. Pre-commit runs against a snapshot of the staged index and pre-push against the pushed commit tree, selected with `WORKSPACE_POE_GIT_SCOPE`. CI uses the pull request's base commit and remains authoritative.

### AnyIO compatibility matrix

CI tests Python 3.11 through 3.15 against the lowest and latest AnyIO versions allowed by the queue SDK's declared dependency range and the workspace's uv configuration.

[[scripts/anyio_version.py#resolve_boundary]] runs `uv pip compile` from the repository root, preserving `exclude-newer`, package-specific exceptions, and index settings from [[pyproject.toml]]. Both the test matrix and bundled-wheel checks use this resolver. [[tests/unit/test_anyio_version.py#test_boundary_respects_project_config|Regression tests]] resolve against a local index with upload timestamps to cover recent releases, exceptions, range bounds, and unsatisfiable cutoffs independently of PyPI.

## Releases

Packages are released independently, with version bumps derived from news fragments and workspace dependency cascades.

Release mechanics are documented in [[RELEASING.md]]. Package discovery and ordering come from uv workspace metadata via [[scripts/workspace.py#packages]] and [[scripts/workspace.py#topological_names]], never from a hard-coded package list.

### News fragments

Every user-visible package change carries a fragment at `changes/<package>/<id>.<type>.md`, which becomes exactly one changelog entry.

Types map to bumps: `breaking` is major (minor while a package is `0.x`), `feature` is minor, and `bugfix`, `docs`, and `internal` are patch; the largest bump per package wins. [[scripts/release.py#compute_releases]] adds dependency-only patch releases for dependents whose workspace lower bounds must move.

Packages at the unpublished `0.0.0` sentinel are exempt from enforcement and cascades; their first fragment promotes them to `0.1.0`. A package adopting a distribution name that already has PyPI releases declares the last one as `previous-version`, and [[scripts/release.py#previous_version]] makes its first release continue from there. CI enforces fragments with `uv run poe check-news-fragments`.

### Dynamic dependencies

Published dependency lists are generated at build time so workspace lower bounds always match the sibling versions being released.

Packages declare dependencies under `[tool.vercel-sdk.release.dependencies]` and mark `project.dependencies` dynamic. [[scripts/hatch_build.py#WorkspaceDependenciesMetadataHook]] rewrites workspace requirements to the current sibling versions while preserving markers, extras, and upper bounds. Local development resolves the same packages through `[tool.uv.sources]` workspace links.

The array at `tool.vercel-sdk.release.dependencies.dependencies` is intentionally nested: [[scripts/bundle_release.py#_rewrite_pyproject]] replaces the `[tool.vercel-sdk.release.dependencies]` header and the array directly below it as text when generating bundles. Other release settings, such as `previous-version`, stay in the parent table, outside that replacement, so their comments and formatting survive without reserializing the TOML file.

Each package's `hatch_build.py` only locates that shared hook and runs it. A source archive and a generated bundle carry their own copy, and [[src/vercel-drives/hatch_build.py#_shared_hook_path]] runs that copy and nothing else, because the directories above an unpacked archive may be writable by other users. Only a checkout has no packaged copy, and there [[src/vercel-drives/hatch_build.py#_declared_hook_source]] runs the file the package's own sdist `force-include` ships as that copy. The location is declared once, inside the package, and is never discovered: uv passes a build hook nothing that names the workspace root, and an ancestor's `[tool.uv.workspace]` claiming the package is not authority to run its code. [[tests/unit/test_release_system.py#test_unpacked_source_archive_never_runs_a_hook_from_outside_it|One regression test]] plants a hook above an unpacked archive, and [[tests/unit/test_release_system.py#test_checkout_runs_only_the_hook_its_own_pyproject_names|another]] plants a workspace and hook in a shared `src/` directory; neither hook runs.

The workspace root itself is not buildable: [[scripts/no_root_build.py]] is its build backend and always fails.

### Publishing

Merged release PRs are published by GitHub Actions through PyPI trusted publishing; maintainers never upload from their machines.

[[scripts/publish.py]] has `detect`, `build`, and `publish` stages that communicate through a JSON plan. Every package builds and passes installed-wheel checks before any upload. Release PRs carry the `release` label, which makes CI run the same stages with `--dry-run`.

Packages opt in to `-bundle` variants with `[tool.vercel-sdk.release.bundle] enabled = true`. [[scripts/bundle_release.py#is_vendored_eligible]] rejects unsupported enabled layouts, and [[scripts/bundle_release.py#build_bundle_package]] assembles the variants. The common HTTP stack is vendored once into a shared `vercel-internal-shared-vendored-deps` distribution so bundles do not each carry a copy. AnyIO and the integrated frameworks stay ordinary peer dependencies because they share state with application code.

[[scripts/bundle_release.py#validate_bundle_packages]] requires every workspace dependency of an enabled bundle to provide its own bundle. [[scripts/build.sh]] and [[scripts/publish.py#dependency_graph]] validate this before building or planning releases. Integrations and their selected SDK dependencies enable eleven variants; Blob, Connect, Drives, Passport, and Sandbox no longer publish bundles.

Libraries with required compiled extensions cannot be vendored, because bundling drops `*.so` files: Pydantic needs `pydantic-core`, and cbor2 needs its `_cbor2` module. Packages declare these exclusions with `external-dependencies` in their bundle tables, read by [[scripts/bundle_release.py#_external_and_peer_dependencies]].

[[.github/scripts/test_installed_wheel.sh]] detects vendored HTTP clients in the package and its local bundle dependency wheels. It adapts test imports and excludes respx-dependent tests because respx patches only unvendored transports; workspace tests retain that coverage.

## Knowledge graph checks

This `lat.md/` graph is validated in CI so links into source code and between sections cannot silently rot.

The `lat.md check` job in [[.github/workflows/ci.yml]] runs Lat's checker on every push and pull request, and **CI Overall** requires it to pass. Locally, run `lat check` after changing documented code or these files; agent-facing workflow instructions live in the generated section of [[AGENTS.md]].
