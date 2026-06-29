#!/usr/bin/env python3
from __future__ import annotations

from workspace_packages import ROOT, workspace_packages


for path in workspace_packages().values():
    print(path.relative_to(ROOT))
print("tests")
print("examples")
