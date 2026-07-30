Don't require `VERCEL_DEPLOYMENT_ID` to be set when running against a
`vercel dev` queue broker.

This will allow us to revert `vc dev` setting `VERCEL_DEPLOYMENT_ID`.
