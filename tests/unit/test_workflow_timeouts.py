from pathlib import Path
from typing import Any

import pytest
import yaml

WORKFLOWS = Path(__file__).parents[2] / ".github" / "workflows"

JOBS = {
    f"{path.name}:{name}": job
    for path in sorted(WORKFLOWS.glob("*.yml"))
    for name, job in yaml.safe_load(path.read_text(encoding="utf-8"))["jobs"].items()
}


@pytest.mark.parametrize("job", list(JOBS.values()), ids=list(JOBS))
def test_every_job_has_a_bounded_timeout(job: dict[str, Any]) -> None:
    # GitHub has no repository setting for a default job timeout, so each job
    # must cap itself; otherwise a hang runs to the 6-hour platform default.
    timeout = job.get("timeout-minutes")
    assert isinstance(timeout, int)
    assert 0 < timeout <= 60
