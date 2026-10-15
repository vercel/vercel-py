This directory defines the high-level concepts, business logic, and architecture of this project using markdown. It is managed by [lat.md](https://www.npmjs.com/package/lat.md) — a tool that anchors source code to these definitions. Install the `lat` command with `npm i -g lat.md` and run `lat --help`.

- [[architecture]] — how the workspace splits the `vercel` namespace across independently released distributions.
- [[runtime]] — shared runtime conventions: sync/async parity, transports, SDK sessions, request headers, and OIDC credentials.
- [[packages]] — one document per distribution: what each package owns, how it works, and why.
- [[tooling]] — workspace tasks, news fragments, releases, publishing, and knowledge graph validation.
