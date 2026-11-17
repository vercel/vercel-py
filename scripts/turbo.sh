#!/bin/sh
# The Turborepo version used by CI and local runs.
exec npx --yes turbo@2.11.11 "$@"
