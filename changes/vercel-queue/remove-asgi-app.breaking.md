Remove the `asgi_app`, `QueueClientAsgiApp`, `QueueClient.asgi_app()`,
`QueueClientAsgiDevServer`, and `queue_client_asgi_dev_server` APIs. Deploy
queue handlers with `[[tool.vercel.subscribers]]` declarations instead.
