The REST API clients (`vercel.deployments`, `vercel.projects`,
`vercel.project_routes`, and `vercel.client`) now ship in the separate
`vercel-platform` distribution, which `vercel` depends on, so their imports keep
working without installing anything extra.
