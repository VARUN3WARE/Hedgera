"""Guards Phase 0 inventory module (no production coupling)."""

from backend.src.development import PIPELINE_SCRIPTS, ORCHESTRATION_ENGINES


def test_pipeline_scripts_are_unique_paths():
    paths = [s.path for s in PIPELINE_SCRIPTS]
    assert len(paths) == len(set(paths))


def test_docker_default_single():
    defaults = [s for s in PIPELINE_SCRIPTS if s.docker_default]
    assert len(defaults) == 1
    assert defaults[0].path == "parallel_full_pipeline_clean.py"


def test_orchestration_engine_rows():
    assert len(ORCHESTRATION_ENGINES) == 3
