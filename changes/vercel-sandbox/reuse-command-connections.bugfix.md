Reuse pooled connections for `run_command` and command log streams. These requests no longer send `Connection: close`, so each command no longer pays for a new TCP and TLS handshake.
