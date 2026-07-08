# Large File Stream Queue app

Deploy this directory as a Vercel app:

```bash
cd src/vercel-queue/examples/large_file_stream
vc link
vc deploy
```

Create a small local file, then send it from an environment that has Vercel
Queue credentials. Use `--region iad1` for local sends outside Vercel:

```bash
printf 'example file payload\n' > /tmp/queue-file.bin
uv run --project ../.. python -m vercel.queue send --region iad1 --topic files --binary-from /tmp/queue-file.bin
```

Check the function logs:

```bash
vc logs <deployment-url>
```

Expected output includes:

```text
received 21 bytes
```

The push subscriber consumes the binary stream without buffering the whole
payload up front.
