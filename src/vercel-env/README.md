# Vercel Environment

`vercel.env` provides Vercel system environment variable parsing and helpers.

## Installation

```sh
pip install vercel-env
```

## Usage

```python
from vercel.env import get_env

env = get_env()
print(env.VERCEL_ENV)
```
