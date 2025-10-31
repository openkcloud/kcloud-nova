"""
Accelerator Weigher with regex-based RC detection and harmonic combining
------------------------------------------------------------------------
- Supports "one device per child RP" layout (root = compute RP).
- Matches accelerator RCs via regex pattern (e.g., ^CUSTOM_.*(NPU|AICHIP).*$).
- Aggregates free amounts (total - reserved - used) for matched RCs
  across all child RPs of each compute node.
- Combines multiple RCs using 'harmonic' (default) or 'min' modes.
- Queries Placement directly; no caching.
"""

from nova.scheduler import weights
from oslo_config import cfg
from oslo_log import log as logging

from keystoneauth1 import loading as ks_loading
from keystoneauth1 import session as ks_session
from keystoneauth1.adapter import Adapter

import re
from typing import Dict, List, Optional

LOG = logging.getLogger(__name__)

_ACCEL_OPTS = [
    cfg.StrOpt(
        "rc_pattern",
        default="(?i)^(CUSTOM_)?(FPGA|PGPU|VGPU|QAT|NIC|SSD|AICHIP)$",
        help="Regex pattern to match accelerator resource classes",
    ),
    cfg.FloatOpt(
        "weight_multiplier",
        default=1.0,
        help="Multiplier applied to the final score.",
    ),
    cfg.IntOpt(
        "http_timeout",
        default=3,
        min=1,
        help="HTTP timeout (seconds) for Placement API calls.",
    ),
    cfg.BoolOpt(
        "prefer_more_free",
        default=True,
        help="If True, more free capacity -> higher score; if False, reverse.",
    ),
    cfg.IntOpt(
        "in_tree_page_limit",
        default=1000,
        min=100,
        help="Pagination limit for in_tree listing.",
    ),
    cfg.StrOpt(
        "combine",
        default="harmonic",
        choices=["harmonic", "min"],
        help="How to combine multi-RC slack within a request group.",
    ),
]

CONF = cfg.CONF
CONF.register_opts(_ACCEL_OPTS, group="accelerator_weigher")


# ------------------------------ Placement utils ------------------------------

def _build_placement_adapter() -> Adapter:
    """Create a Keystone session and Adapter for Placement API."""
    auth = ks_loading.load_auth_from_conf_options(CONF, "placement")
    sess = ks_session.Session(auth=auth, timeout=CONF.accelerator_weigher.http_timeout)
    return Adapter(
        session=sess,
        service_type="placement",
        interface=getattr(CONF.placement, "valid_interfaces", None)
                  or getattr(CONF.placement, "interface", None)
                  or "public",
        region_name=getattr(CONF.placement, "region_name", None),
        endpoint_override=getattr(CONF.placement, "endpoint_override", None),
    )


def _lookup_root_rp_uuid(adapter: Adapter, host_state) -> Optional[str]:
    """Find compute RP UUID by hypervisor name."""
    name = getattr(host_state, "hypervisor_hostname", None) or getattr(host_state, "host", None)
    if not name:
        return None

    resp = adapter.get("/resource_providers", params={"name": name})
    if resp.status_code != 200:
        LOG.debug("RP lookup failed for %s: %s %s", name, resp.status_code, resp.text)
        return None

    rps = (resp.json() or {}).get("resource_providers", [])
    if not rps:
        return None
    return rps[0].get("uuid")


def _iter_in_tree(adapter: Adapter, root_uuid: str, limit: int):
    """Yield all RPs in the tree rooted at a given compute RP."""
    if not root_uuid:
        return
    marker = None
    while True:
        params = {"in_tree": root_uuid, "limit": limit}
        if marker:
            params["marker"] = marker
        resp = adapter.get("/resource_providers", params=params)
        if resp.status_code != 200:
            LOG.debug("in_tree query failed root=%s: %s %s", root_uuid, resp.status_code, resp.text)
            return
        rps = (resp.json() or {}).get("resource_providers", [])
        if not rps:
            return
        for rp in rps:
            yield rp
        marker = rps[-1].get("uuid")
        if len(rps) < limit:
            return


def _get_free_from_inventory(adapter: Adapter, rp_uuid: str, rc_name: str) -> float:
    """Return free = max(total - reserved - used, 0) for one RC on a given RP."""
    inv = adapter.get(f"/resource_providers/{rp_uuid}/inventories/{rc_name}")
    if inv.status_code == 404:
        return 0.0
    if inv.status_code != 200:
        LOG.debug("Inventory fetch failed rp=%s rc=%s: %s %s",
                  rp_uuid, rc_name, inv.status_code, inv.text)
        return 0.0

    inv_body = inv.json() or {}
    total = float(inv_body.get("total", 0))
    reserved = float(inv_body.get("reserved", 0))

    usage = adapter.get(f"/resource_providers/{rp_uuid}/usages")
    if usage.status_code != 200:
        LOG.debug("Usage fetch failed rp=%s: %s %s", rp_uuid, usage.status_code, usage.text)
        used = 0.0
    else:
        used = float((usage.json() or {}).get("usages", {}).get(rc_name, 0))

    free = total - reserved - used
    return free if free > 0 else 0.0


def _list_rcs_for_rp(adapter: Adapter, rp_uuid: str, rc_pattern: re.Pattern) -> List[str]:
    """Return list of RCs in this RP that match the regex pattern."""
    resp = adapter.get(f"/resource_providers/{rp_uuid}/inventories")
    if resp.status_code != 200:
        LOG.debug("Failed to list inventories for RP=%s: %s %s",
                  rp_uuid, resp.status_code, resp.text)
        return []
    invs = (resp.json() or {}).get("inventories", {})
    return [rc for rc in invs.keys() if rc_pattern.match(rc)]


def _sum_free_for_matching_rcs(adapter: Adapter, root_uuid: str, rc_pattern: re.Pattern) -> Dict[str, float]:
    """Aggregate total free amounts for all RCs matching regex across the RP tree."""
    totals: Dict[str, float] = {}
    limit = CONF.accelerator_weigher.in_tree_page_limit

    for rp in _iter_in_tree(adapter, root_uuid, limit=limit):
        rp_uuid = rp.get("uuid")
        if not rp_uuid or rp_uuid == root_uuid:
            continue

        rc_list = _list_rcs_for_rp(adapter, rp_uuid, rc_pattern)
        for rc in rc_list:
            free = _get_free_from_inventory(adapter, rp_uuid, rc)
            totals[rc] = totals.get(rc, 0.0) + free

    return totals


def _combine_rc_totals(totals: Dict[str, float]) -> float:
    """Combine RC totals into one scalar using harmonic or min mode."""
    if not totals:
        return 0.0

    mode = CONF.accelerator_weigher.combine.lower()
    values = [v for v in totals.values() if v > 0]

    if not values:
        return 0.0

    if mode == "min":
        return min(values)

    # Harmonic mean: n / Σ(1/x_i)
    try:
        n = len(values)
        harmonic = n / sum(1.0 / v for v in values)
        return harmonic
    except ZeroDivisionError:
        return 0.0


# ------------------------------ Main Weigher ------------------------------

class AcceleratorWeigher(weights.BaseHostWeigher):
    """
    Nova weigher that scores hosts based on harmonic or min combination
    of accelerator availability across all child RPs.
    """
    _adapter: Optional[Adapter] = None

    def _ensure_adapter(self):
        if self._adapter is None:
            try:
                self._adapter = _build_placement_adapter()
            except Exception:
                LOG.exception("Failed to build Placement adapter for AcceleratorWeigher")
                self._adapter = None

    def weight_multiplier(self):
        return CONF.accelerator_weigher.weight_multiplier

    def _weigh_object(self, host_state, weight_properties):
        """Compute the host weight based on RCs matching rc_pattern."""
        LOG.debug("AcceleratorWeigher _weigh_object() is called")
        self._ensure_adapter()
        if not self._adapter:
            return 0.0

        rc_pattern = re.compile(CONF.accelerator_weigher.rc_pattern)
        root_uuid = _lookup_root_rp_uuid(self._adapter, host_state)
        totals = _sum_free_for_matching_rcs(self._adapter, root_uuid, rc_pattern)

        combined = _combine_rc_totals(totals)
        score = float(combined if CONF.accelerator_weigher.prefer_more_free else -combined)

        LOG.debug(
            "AcceleratorWeigher host=%s root_rp=%s matched_rcs=%s totals=%s combined=%.3f score=%.3f",
            getattr(host_state, "host", "?"),
            root_uuid,
            list(totals.keys()),
            totals,
            combined,
            score,
        )
        return score
