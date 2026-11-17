The function helpers (`vercel.functions` and `vercel.env`) now ship in the
separate `vercel-context` distribution, which `vercel` depends on, so their
imports keep working without installing anything extra.
