# -*- coding: utf-8 -*-
"""
Accelerator Weigher (Placement via SchedulerReportClient, version-safe)
----------------------------------------------------------------------
- One-device-per-child-RP layout (root = compute RP).
- Detects accelerator RCs via regex (FPGA/PGPU/VGPU/CUSTOM_QAT/NIC/SSD/AICHIP).
- Aggregates free amounts (total - reserved - used) across all child RPs.
- Combines multi-RC availability with 'harmonic' (default) or 'min'.
- Uses Nova's SchedulerReportClient singleton and safely handles different
  get_providers_in_tree() signatures across Nova versions.
"""

import re
from urllib.parse import urlencode
from typing import Dict, List, Optional

from oslo_config import cfg
from oslo_log import log as logging

from nova import context as nova_context
from nova.scheduler import weights
from nova.scheduler.client import report as placement_report

LOG = logging.getLogger(__name__)

_ACCEL_OPTS = [
    cfg.StrOpt(
        "rc_pattern",
        default=r"(?i)^(CUSTOM_)?(FPGA|PGPU|VGPU|QAT|NIC|SSD|AICHIP)$",
        help=(
            "Regex to match accelerator resource classes. "
            "Matches: FPGA, PGPU, VGPU, CUSTOM_QAT, CUSTOM_NIC, CUSTOM_SSD, CUSTOM_AICHIP."
        ),
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
        help="HTTP timeout (seconds) if honored by client (client handles retries).",
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
        help="Pagination limit for manual /resource_providers?in_tree=... fallback.",
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


# ------------------------------ Placement helpers (via SchedulerReportClient) ------------------------------

def _get_client() -> placement_report.SchedulerReportClient:
    """Return the global singleton SchedulerReportClient instance."""
    return placement_report.report_client_singleton()


def _client_get(client: placement_report.SchedulerReportClient, path: str, params: Optional[Dict] = None):
    """Call client.get() (or _get()) with querystring embedded in the URL (no 'params' kw)."""
    getter = getattr(client, "get", None) or getattr(client, "_get")
    if params:
        qs = urlencode(params, doseq=True)
        path = f"{path}?{qs}"
    return getter(path)


def _lookup_root_rp_uuid(client: placement_report.SchedulerReportClient, host_state) -> Optional[str]:
    """Find compute RP UUID by hypervisor name via /resource_providers?name=<hypervisor_hostname>."""
    name = getattr(host_state, "hypervisor_hostname", None) or getattr(host_state, "host", None)
    if not name:
        return None

    LOG.debug("_lookup_root_rp_uuid:name: %s", name)
    resp = _client_get(client, "/resource_providers", params={"name": name})
    if resp.status_code != 200:
        LOG.debug("RP lookup failed for %s: %s %s", name, resp.status_code, resp.text)
        return None

    data = resp.json() or {}
    LOG.debug("_lookup_root_rp_uuid:data: %s", data)
    rps = data.get("resource_providers", []) or []
    return rps[0].get("uuid") if rps else None


def _providers_in_tree_safe(client: placement_report.SchedulerReportClient, root_uuid: str) -> List[Dict]:
    """Return providers in tree using version-safe calls.
    Tries known signatures of get_providers_in_tree(), then falls back to manual REST.
    """
    # 1) Common in many releases (e.g., stable/2024.1): get_providers_in_tree(context, uuid)
    try:
        ctx = nova_context.get_admin_context()
        providers = client.get_providers_in_tree(ctx, root_uuid)  # context unused; None is fine
        if providers:
            return providers
    except TypeError:
        # Signature mismatch; try other variants
        pass
    except Exception:
        LOG.debug("get_providers_in_tree(context, uuid) failed", exc_info=True)

    # 2) Some releases: get_providers_in_tree(uuid) or get_providers_in_tree(uuid, include_root=...)
    try:
        providers = client.get_providers_in_tree(root_uuid)  # no context
        if providers:
            return providers
    except TypeError:
        pass
    except Exception:
        LOG.debug("get_providers_in_tree(uuid) failed", exc_info=True)

    # 3) Fallback: manual REST GET /resource_providers?in_tree=<uuid> (no paging here; add if needed)
    try:
        resp = _client_get(client, "/resource_providers", params={"in_tree": root_uuid})
        if resp.status_code == 200:
            return (resp.json() or {}).get("resource_providers", []) or []
        LOG.debug("Fallback in_tree GET failed: %s %s", resp.status_code, resp.text)
    except Exception:
        LOG.debug("Fallback manual in_tree GET raised", exc_info=True)

    return []


def _list_matching_rcs_for_rp(
    client: placement_report.SchedulerReportClient,
    rp_uuid: str,
    rc_regex: re.Pattern,
) -> List[str]:
    """List RC names present in RP's inventories that match the regex."""
    resp = _client_get(client, f"/resource_providers/{rp_uuid}/inventories")
    LOG.debug("_list_matching_rcs_for_rp:resp: %s", resp.json())
    if resp.status_code != 200:
        LOG.debug("Failed to list inventories for RP=%s: %s %s", rp_uuid, resp.status_code, resp.text)
        return []
    invs = (resp.json() or {}).get("inventories", {}) or {}
    return [rc for rc in invs.keys() if rc_regex.match(rc)]


def _get_free_for_rc(
    client: placement_report.SchedulerReportClient,
    rp_uuid: str,
    rc_name: str,
) -> float:
    """Compute free = max(total - reserved - used, 0) for given RP and RC."""
    inv = _client_get(client, f"/resource_providers/{rp_uuid}/inventories/{rc_name}")
    if inv.status_code == 404:
        return 0.0
    if inv.status_code != 200:
        LOG.debug("Inventory fetch failed rp=%s rc=%s: %s %s", rp_uuid, rc_name, inv.status_code, inv.text)
        return 0.0

    inv_body = inv.json() or {}
    total = float(inv_body.get("total", 0))
    reserved = float(inv_body.get("reserved", 0))

    usage = _client_get(client, f"/resource_providers/{rp_uuid}/usages")
    if usage.status_code != 200:
        LOG.debug("Usage fetch failed rp=%s: %s %s", rp_uuid, usage.status_code, usage.text)
        used = 0.0
    else:
        used = float((usage.json() or {}).get("usages", {}).get(rc_name, 0))

    free = total - reserved - used
    return free if free > 0 else 0.0


def _sum_free_by_rc_in_tree(
    client: placement_report.SchedulerReportClient,
    root_uuid: str,
    rc_regex: re.Pattern,
) -> Dict[str, float]:
    """Aggregate total free amounts per RC across all *child* RPs (root excluded)."""
    totals: Dict[str, float] = {}

    for rp in _providers_in_tree_safe(client, root_uuid):
        rp_uuid = rp.get("uuid")
        if not rp_uuid or rp_uuid == root_uuid:
            # Root compute RP typically holds CPU/MEM/DISK, devices live on children.
            continue

        rcs = _list_matching_rcs_for_rp(client, rp_uuid, rc_regex)
        for rc in rcs:
            totals[rc] = totals.get(rc, 0.0) + _get_free_for_rc(client, rp_uuid, rc)

    return totals


def _combine_totals(totals: Dict[str, float]) -> float:
    """Combine per-RC totals into a single scalar using 'harmonic' or 'min'."""
    if not totals:
        return 0.0

    mode = (CONF.accelerator_weigher.combine or "harmonic").lower()
    values = [v for v in totals.values() if v > 0.0]
    if not values:
        return 0.0

    if mode == "min":
        return min(values)

    # harmonic mean = n / sum(1/x_i)
    try:
        n = len(values)
        return n / sum(1.0 / v for v in values)
    except ZeroDivisionError:
        return 0.0


# ------------------------------ Main Weigher ------------------------------

class AcceleratorWeigher(weights.BaseHostWeigher):
    """
    Weigher that scores hosts by accelerator availability across child RPs.
    - RCs are matched by regex (rc_pattern).
    - Totals are combined using 'harmonic' or 'min'.
    """

    def weight_multiplier(self, host_state):
        LOG.debug("host_state: %s", dir(host_state))
        return CONF.accelerator_weigher.weight_multiplier

    def _weigh_object(self, host_state, weight_properties):
        """Compute the host weight using Placement via SchedulerReportClient."""
        LOG.debug("host_state: %s", dir(host_state))
        LOG.debug("weight_properties: %s", dir(weight_properties))

        client = _get_client()

        rc_regex = re.compile(CONF.accelerator_weigher.rc_pattern)

        root_uuid = _lookup_root_rp_uuid(client, host_state)
        if not root_uuid:
            LOG.debug("No root RP for host=%s; returning 0", getattr(host_state, "host", "?"))
            return 0.0

        totals = _sum_free_by_rc_in_tree(client, root_uuid, rc_regex)
        combined = _combine_totals(totals)

        score = float(combined if CONF.accelerator_weigher.prefer_more_free else -combined)
        LOG.debug(
            "AcceleratorWeigher host=%s root_rp=%s rcs=%s totals=%s combined=%.3f score=%.3f",
            getattr(host_state, "host", "?"),
            root_uuid,
            list(totals.keys()),
            totals,
            combined,
            score,
        )
        return score

