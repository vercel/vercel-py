# Changelog

## 0.7.0 - 2026-07-13

### Features

- Add an experimental `vercel.unstable.sandbox` SDK with sync and async sandbox lifecycle, process, filesystem, snapshot, query, and session APIs. (#128) (#172)
- Add streaming read and write support for Sandbox files and process output. (#135) (#172)
- Improve Workflows sandbox execution with configurable cleanup handlers, passthrough modules, namespaced workflows, and more reliable resume/error handling. (#144, #146, #148, #153, #154, #155, #156, #162, #163, #164, #165) (#172)
- Refactor internal HTTP transport handling to support streamed responses, raw bodies, timeouts, retries, and shared token resolution. (#119) (#172)
