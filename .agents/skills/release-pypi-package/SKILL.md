---
name: release-pypi-package
description: Publish a package from this uv workspace to PyPI using package-version tags, signed annotated release tags, the Publish PyPI workflow, and concise GitHub release notes. Use when cutting or drafting releases for workspace packages such as vercel, vercel-headers, or vercel-oidc.
---

# Release PyPI Package

Use this skill to publish one package from this repository's uv workspace.
Package releases are driven by tags named `{package_name}-v{version}`.

Assume repository tag rulesets are in place and treat release tags as protected:
do not retarget, force-push, or delete a release tag unless the user explicitly
asks for incident recovery.

## Workflow

1. Ground the release.
   - Fetch tags and prune stale refs: `git fetch origin --tags --prune`.
   - From a clean worktree at the release commit, confirm the package exists in
     the workspace and read its version with
     `python scripts/get-version.py {package_name}`.
   - Confirm the expected tag does not already exist locally or remotely.
   - Review changes since the previous release tag for the same package.

2. Create the signed tag.
   - Use an annotated signed tag at the release commit:
     `git tag -s {package_name}-v{version} {commit} -m "{package_name} v{version}" -m "{short tag note}"`.
   - Keep tag notes terse. Good examples: `Initial release`,
     `Initial standalone release`, `Workspace package release`.
   - Verify with `git tag -v {package_name}-v{version}` and inspect
     `git show --no-patch --format=fuller {package_name}-v{version}`.
   - Push only the tag: `git push origin refs/tags/{package_name}-v{version}`.

3. Verify publishing.
   - Check the `Publish PyPI` workflow for the tag.
   - Confirm it completed successfully before creating or announcing the
     GitHub release.

4. Create the GitHub release.
   - Title format is exactly `{package_name} v{version}` with no Markdown
     backticks.
   - Keep the body concise and sectioned by area. Do not add a generic
     `Release notes` heading because the release body already is release notes.
   - Use prior release style: short bullets with PR numbers are preferred over
     long prose.
   - Create the release with `gh release create {tag} --verify-tag --title
     "{package_name} v{version}" --notes-file {notes_file}`.
   - Verify with `gh release view {tag}`.

## Latest Pointer

Only care about moving the GitHub Releases `latest` pointer when releasing the
workspace package itself, currently `vercel`.

For split-out support packages such as `vercel-headers` and `vercel-oidc`, create
the tag and release as needed for publication and documentation, but do not spend
time trying to make them the repository's latest release unless the user asks.

## Release Notes Shape

Prefer this form:

```md
## Packaging

- Convert the repo to a uv workspace and split `vercel.headers` and
  `vercel.oidc` into standalone packages while keeping them available through
  `vercel` (#149)

## Sandbox

- Add `image` support for Sandbox creation (#147)
```

Use section names that match the change set, such as `Packaging`, `Sandbox`,
`Workflow`, `Cache`, `Blob`, or `Fixes`.
