import ast
import re
from pathlib import Path

from smartdialer.v3.pacing import PacingInputs, propose

PACKAGE = Path(__file__).resolve().parents[2] / "smartdialer" / "v3"


def test_pacing_imports_only_standard_library() -> None:
    pacing = ast.parse((PACKAGE / "pacing.py").read_text(encoding="utf-8"))
    imports = []
    for node in ast.walk(pacing):
        if isinstance(node, ast.ImportFrom):
            assert node.level == 0 and node.module is not None
            imports.append(node.module)
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
    assert set(imports) <= {"dataclasses", "math"}


def test_only_call_transition_service_inserts_calls_with_approval() -> None:
    inserts = []
    for path in PACKAGE.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                match = re.search(r"INSERT INTO calls\s*\(([^)]+)\)", node.value, flags=re.IGNORECASE)
                if match:
                    assert "safety_decision_id" in match[1]
                    inserts.append(path.name)
    assert inserts == ["calls.py"]


def test_progressive_pacing_is_capped_and_explainable() -> None:
    proposal = propose(PacingInputs(free_agents=50, batch_limit=10))
    assert proposal.requested_calls == 10
    assert proposal.reasoning == "free=50, batch_limit=10, request=10"
    assert propose(PacingInputs(free_agents=0, batch_limit=10)).requested_calls == 0
