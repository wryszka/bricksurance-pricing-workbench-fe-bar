"""Cross-field sanity checks in the MCP/broker quote validation.

The pricing agents (broker chat + MCP surface) both gate on `validate()`, so these
deterministic checks stop obviously-impossible inputs from being priced regardless of
what the LLM extracts — e.g. a 17-year-old with 10 no-claims years."""
import sys
from pathlib import Path

APP = Path(__file__).resolve().parents[1] / "src" / "app"
if str(APP) not in sys.path:
    sys.path.insert(0, str(APP))

from server.mcp_tools import validate  # noqa: E402

_BASE = {
    "driver_age": 40, "license_years_held": 20, "no_claims_years": 15,
    "occupation_class": "Office", "vehicle_value": 20000, "vehicle_group": 20,
    "annual_mileage": 8000, "vehicle_age": 3, "postcode_area_risk": "medium",
    "cover_type": "comprehensive", "voluntary_excess": 250, "claim_count_5y": 0,
    "parking_overnight": "Drive",
}


def _errors(overrides):
    _, errors = validate({**_BASE, **overrides})
    return errors


def test_17yo_with_10_ncd_is_rejected():
    errs = _errors({"driver_age": 17, "license_years_held": 1, "no_claims_years": 10})
    assert any("no_claims_years" in e and "impossible" in e for e in errs)


def test_license_years_cannot_exceed_age_minus_17():
    errs = _errors({"driver_age": 40, "license_years_held": 30})
    assert any("license_years_held" in e and "impossible" in e for e in errs)


def test_ncd_cannot_exceed_license_years():
    errs = _errors({"license_years_held": 5, "no_claims_years": 15})
    assert any("cannot exceed license_years_held" in e for e in errs)


def test_recent_claims_with_high_ncd_warns():
    errs = _errors({"no_claims_years": 15, "claim_count_5y": 10})
    assert any(e.startswith("WARNING") and "inconsistent" in e for e in errs)


def test_plausible_case_has_no_crossfield_errors():
    _, errors = validate(_BASE)
    # No impossibility/inconsistency errors on a sane driver.
    assert not [e for e in errors if "impossible" in e or "cannot exceed" in e or e.startswith("WARNING")]
