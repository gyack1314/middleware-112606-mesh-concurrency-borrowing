"""Compatibility-aware admission service for shared workbench replicas.

The service is intentionally deterministic and standard-library only.  It keeps
execution facts until an endpoint reports a definite terminal result, models
skill-compatible placement with capacitated matching, and explains why a live
tenant guarantee is not effective yet.

The usual integration entry point is :class:`AdmissionService`.  The JSONL
drill format from ``README.md`` can be replayed with :func:`replay_jsonl` or
``python admission_service.py examples``.
"""

from __future__ import annotations

import argparse
import functools
import json
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional


LIVE_CLASSES = {"live", "online"}
BATCH_CLASSES = {"batch", "background"}
TERMINAL_STATES = {"done", "stopped", "expired"}
DEFINITE_RESULTS = {"done", "stopped"}
NON_TERMINAL_RESULTS = {"running", "unknown"}
SUPPORTED_KINDS = {
    "accept", "endpoint", "inspect", "request", "restart",
    "result", "sent", "stop", "tenant", "tick",
}


class AdmissionError(ValueError):
    """Raised when a message violates the admission state contract."""


@dataclass
class AdmissionDecision:
    admitted: bool
    request_id: Optional[str] = None
    at: Optional[int] = None
    permit_id: Optional[str] = None
    endpoint: Optional[str] = None
    reason: Optional[str] = None
    detail: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "admitted": self.admitted,
            "request_id": self.request_id,
            "at": self.at,
            "permit_id": self.permit_id,
            "endpoint": self.endpoint,
            "reason": self.reason,
            "detail": self.detail,
        }


@dataclass
class TickResult:
    at: int
    dispatches: list[dict[str, Any]] = field(default_factory=list)
    stops: list[dict[str, Any]] = field(default_factory=list)
    expired: list[str] = field(default_factory=list)
    blocked: list[dict[str, Any]] = field(default_factory=list)
    blocked_for_fairness: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "at": self.at,
            "dispatches": self.dispatches,
            "stops": self.stops,
            "expired": self.expired,
            "blocked": self.blocked,
            "blocked_for_fairness": self.blocked_for_fairness,
        }


class _Edge:
    __slots__ = ("to", "rev", "cap")

    def __init__(self, to: int, rev: int, cap: int) -> None:
        self.to = to
        self.rev = rev
        self.cap = cap


def _max_flow_assignment(
    request_skills: list[tuple[str, str]],
    endpoint_caps: dict[str, int],
    endpoint_skills: dict[str, set[str]],
) -> tuple[int, dict[str, str]]:
    """Maximum bipartite flow; returns flow and one request->endpoint matching."""

    endpoints = list(endpoint_caps)
    n_req = len(request_skills)
    req_start = 1
    ep_start = req_start + n_req
    sink = ep_start + len(endpoints)
    n_nodes = sink + 1
    graph: list[list[_Edge]] = [[] for _ in range(n_nodes)]

    def add_edge(fr: int, to: int, cap: int) -> _Edge:
        forward = _Edge(to, len(graph[to]), cap)
        backward = _Edge(fr, len(graph[fr]), 0)
        graph[fr].append(forward)
        graph[to].append(backward)
        return forward

    for i, _ in enumerate(request_skills):
        add_edge(0, req_start + i, 1)

    choice_edges: list[tuple[str, _Edge, str]] = []
    for i, (rid, skill) in enumerate(request_skills):
        req_node = req_start + i
        for j, ep in enumerate(endpoints):
            cap = int(endpoint_caps.get(ep, 0) or 0)
            if cap <= 0:
                continue
            if skill not in endpoint_skills.get(ep, set()):
                continue
            edge = add_edge(req_node, ep_start + j, 1)
            choice_edges.append((rid, edge, ep))

    for j, ep in enumerate(endpoints):
        cap = max(0, int(endpoint_caps.get(ep, 0) or 0))
        if cap:
            add_edge(ep_start + j, sink, cap)

    flow = 0
    while True:
        level = [-1] * n_nodes
        level[0] = 0
        queue = [0]
        for node in queue:
            for edge in graph[node]:
                if edge.cap > 0 and level[edge.to] < 0:
                    level[edge.to] = level[node] + 1
                    queue.append(edge.to)
        if level[sink] < 0:
            break

        it = [0] * n_nodes

        def dfs(node: int, pushed: int) -> int:
            if node == sink:
                return pushed
            while it[node] < len(graph[node]):
                edge = graph[node][it[node]]
                if edge.cap > 0 and level[edge.to] == level[node] + 1:
                    moved = dfs(edge.to, min(pushed, edge.cap))
                    if moved:
                        edge.cap -= moved
                        graph[edge.to][edge.rev].cap += moved
                        return moved
                it[node] += 1
            return 0

        while True:
            pushed = dfs(0, 10**9)
            if not pushed:
                break
            flow += pushed

    assignment: dict[str, str] = {}
    for rid, edge, ep in choice_edges:
        # A forward 1-capacity edge has residual capacity zero exactly when used.
        if edge.cap == 0:
            assignment[rid] = ep
    return flow, assignment


def _locked_method(func):
    @functools.wraps(func)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        lock = getattr(self, "_lock", None)
        if lock is None:
            return func(self, *args, **kwargs)
        with lock:
            return func(self, *args, **kwargs)

    return wrapper


def _thread_safe(cls: type[Any]) -> type[Any]:
    for name, attr in list(vars(cls).items()):
        if name.startswith("_"):
            continue
        if isinstance(attr, (staticmethod, classmethod)):
            continue
        if callable(attr):
            setattr(cls, name, _locked_method(attr))
    return cls


@_thread_safe
class AdmissionService:
    def __init__(
        self,
        now: int = 0,
        fairness_delay: int = 3,
        aging_delay: Optional[int] = None,
        **options: Any,
    ) -> None:
        self.now = int(now)
        self._lock = threading.RLock()
        delay = fairness_delay if aging_delay is None else aging_delay
        if options.get("starvation_delay") is not None:
            delay = options["starvation_delay"]
        self.fairness_delay = delay
        self.tenants: dict[str, dict[str, Any]] = {}
        self.endpoints: dict[str, dict[str, Any]] = {}
        self.requests: dict[str, dict[str, Any]] = {}
        self.permits: dict[str, dict[str, Any]] = {}
        self.queue: list[str] = []
        self.events: list[dict[str, Any]] = []
        self.outbox: list[dict[str, Any]] = []
        self.restarts = 0
        self._request_seq = 0
        self._permit_seq = 0
        self.last_result: Optional[TickResult] = None

    # ------------------------------------------------------------------
    # Time and configuration
    # ------------------------------------------------------------------
    def _set_time(self, at: Optional[int]) -> int:
        if at is None:
            return self.now
        at = int(at)
        if at < self.now:
            raise AdmissionError(f"时间点不能倒退：{at} < {self.now}")
        self.now = at
        return at

    @staticmethod
    def _tier(value: Optional[str], default: str = "live") -> str:
        value = value or default
        if value in LIVE_CLASSES:
            return "live"
        if value in BATCH_CLASSES:
            return "batch"
        raise AdmissionError(f"未知租户类型：{value}")

    def upsert_tenant(
        self,
        name: Optional[str] = None,
        guarantee: int = 0,
        tier: Optional[str] = None,
        cls: Optional[str] = None,
        at: Optional[int] = None,
        **extra: Any,
    ) -> dict[str, Any]:
        if name is None:
            raise AdmissionError("tenant 缺少 name")
        now = self._set_time(at)
        old = self.tenants.get(name, {})
        record_class = extra.get("class")
        tier_value = self._tier(cls or tier or record_class or old.get("class", "live"))
        max_wait = extra.get(
            "max_wait",
            extra.get("wait_timeout", extra.get("wait_ttl", extra.get("wait_limit", extra.get("wait")))),
        )
        if max_wait is None:
            max_wait = old.get("max_wait")
        aging_delay = extra.get(
            "aging_delay", extra.get("fairness_delay", extra.get("starvation_wait", old.get("aging_delay", self.fairness_delay)))
        )
        if aging_delay is None:
            aging_delay = self.fairness_delay
        tenant = {
            "name": name,
            "class": tier_value,
            "tier": tier_value,
            "guarantee": max(0, int(guarantee if guarantee is not None else old.get("guarantee", 0))),
            "wait_deadline": extra.get("wait_deadline", old.get("wait_deadline")),
            "max_wait": None if max_wait is None else int(max_wait),
            "aging_delay": None if aging_delay is None else int(aging_delay),
            "updated_at": now,
            "created_at": old.get("created_at", now),
            "config_revision": old.get("config_revision", 0) + 1,
        }
        self.tenants[name] = tenant
        return tenant

    # Friendly aliases.
    add_tenant = upsert_tenant
    register_tenant = upsert_tenant

    def load_tenant(self, record: dict[str, Any], at: Optional[int] = None) -> dict[str, Any]:
        data = dict(record)
        record_at = data.pop("at", at)
        return self.upsert_tenant(at=record_at, **data)

    def load_tenant_record(self, record: dict[str, Any], at: Optional[int] = None) -> dict[str, Any]:
        return self.load_tenant(record, at=at)

    def upsert_endpoint(
        self,
        name: Optional[str] = None,
        skills: Optional[Iterable[str]] = None,
        skill: Optional[Iterable[str] | str] = None,
        cap: Optional[int] = None,
        epoch: int = 1,
        at: Optional[int] = None,
        connected: bool = True,
        **extra: Any,
    ) -> dict[str, Any]:
        if name is None:
            raise AdmissionError("endpoint 缺少 name")
        now = self._set_time(at)
        old = self.endpoints.get(name, {})
        state = extra.get("state", extra.get("status"))
        if state in {"disconnected", "down", "offline"}:
            connected = False
        elif state in {"connected", "up", "online"}:
            connected = True
        if skills is None and skill is not None:
            skills = [skill] if isinstance(skill, str) else list(skill)
        if skills is None:
            skill_set = set(old.get("skills", set()))
        else:
            skill_set = {str(s) for s in skills}
        new_cap = int(cap if cap is not None else old.get("cap", 0))
        if new_cap < 0:
            raise AdmissionError("cap 不能为负数")
        cap_until = extra.get(
            "cap_until",
            extra.get(
                "derate_until",
                extra.get("restore_at", extra.get("recover_at", extra.get("until", extra.get("restore_time", old.get("cap_until"))))),
            ),
        )
        normal_cap = extra.get(
            "normal_cap", extra.get("base_cap", extra.get("original_cap", old.get("normal_cap")))
        )
        if cap_until is None and extra.get("duration") is not None:
            cap_until = now + int(extra["duration"])
        if normal_cap is None and cap_until is not None and old.get("cap") is not None:
            normal_cap = old.get("cap")
        endpoint = {
            "name": name,
            "epoch": int(epoch),
            "skills": skill_set,
            "cap": new_cap,
            "current_cap": new_cap,
            "connected": bool(extra.get("connected", old.get("connected", connected))),
            "state": state or ("online" if connected else "disconnected"),
            "cap_until": cap_until,
            "normal_cap": normal_cap,
            "updated_at": now,
            "created_at": old.get("created_at", now),
        }
        self.endpoints[name] = endpoint
        return endpoint

    add_instance = upsert_endpoint
    register_instance = upsert_endpoint
    register_endpoint = upsert_endpoint
    upsert_instance = upsert_endpoint
    add_endpoint = upsert_endpoint

    def load_endpoint(self, record: dict[str, Any], at: Optional[int] = None) -> dict[str, Any]:
        data = dict(record)
        record_at = data.pop("at", at)
        return self.upsert_endpoint(at=record_at, **data)

    def load_instance(self, record: dict[str, Any], at: Optional[int] = None) -> dict[str, Any]:
        return self.load_endpoint(record, at=at)

    def load_endpoint_record(self, record: dict[str, Any], at: Optional[int] = None) -> dict[str, Any]:
        return self.load_endpoint(record, at=at)

    def disconnect_instance(self, name: str, at: Optional[int] = None) -> dict[str, Any]:
        self._set_time(at)
        if name not in self.endpoints:
            raise AdmissionError(f"未知实例：{name}")
        self.endpoints[name]["connected"] = False
        return self.inspect_endpoint(name)

    def reconnect_instance(self, name: str, at: Optional[int] = None) -> dict[str, Any]:
        self._set_time(at)
        if name not in self.endpoints:
            raise AdmissionError(f"未知实例：{name}")
        self.endpoints[name]["connected"] = True
        return self.inspect_endpoint(name)

    # ------------------------------------------------------------------
    # Requests and execution facts
    # ------------------------------------------------------------------
    def submit_request(
        self,
        request_id: Optional[str] = None,
        tenant: Optional[str] = None,
        skill: Optional[str] = None,
        deadline: Optional[int] = None,
        preemptible: bool = False,
        at: Optional[int] = None,
        schedule: bool = True,
        record: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> AdmissionDecision:
        if isinstance(request_id, dict) and record is None:
            record = request_id
            request_id = None
        if isinstance(tenant, dict) and record is None:
            record = tenant
            tenant = None
        if record is not None:
            merged = dict(record)
            merged.update({k: v for k, v in kwargs.items() if v is not None})
            request_id = merged.get("id") or merged.get("request_id") or request_id
            tenant = merged.get("tenant", tenant)
            skill = merged.get("skill", skill)
            deadline = merged.get("deadline", deadline)
            preemptible = merged.get("preemptible", preemptible)
            at = merged.get("at", at)
            schedule = merged.get("schedule", schedule)
            if "ttl" in merged and deadline is None:
                base = int(at if at is not None else self.now)
                deadline = base + int(merged["ttl"])
        elif request_id is None and "id" in kwargs:
            request_id = kwargs["id"]

        if not request_id:
            raise AdmissionError("request 缺少 id")
        if request_id in self.requests:
            raise AdmissionError(f"请求 id 重复：{request_id}")
        if not tenant or tenant not in self.tenants:
            raise AdmissionError(f"请求 {request_id} 使用了未登记租户：{tenant}")
        if not skill:
            raise AdmissionError(f"请求 {request_id} 缺少 skill")

        now = self._set_time(at)
        tenant_rec = self.tenants[tenant]
        wait_budget = tenant_rec.get("max_wait")
        if wait_budget is None and tenant_rec.get("wait_deadline") is not None:
            # A tenant-level wait period is naturally a relative budget;
            # request.deadline remains an explicit absolute deadline.
            wait_budget = tenant_rec.get("wait_deadline")
        if deadline is None and wait_budget is not None:
            deadline = now + int(wait_budget)
        self._request_seq += 1
        req = {
            "id": request_id,
            "tenant": tenant,
            "tier": tenant_rec["class"],
            "skill": skill,
            "deadline": deadline,
            "preemptible": bool(preemptible),
            "submitted_at": now,
            "seq": self._request_seq,
            "status": "queued",
            "permit_id": None,
            "endpoint": None,
            "admitted_at": None,
            "expired_at": None,
            "caller_gone": False,
            "history": [{"at": now, "event": "submitted"}],
        }
        self.requests[request_id] = req
        self.queue.append(request_id)

        if schedule:
            result = self.tick(at=now)
            for dispatch in result.dispatches:
                if dispatch["request_id"] == request_id:
                    return AdmissionDecision(
                        admitted=True,
                        request_id=request_id,
                        at=now,
                        permit_id=dispatch["permit"],
                        endpoint=dispatch["endpoint"],
                        reason="admitted",
                    )
            if self.requests[request_id]["status"] == "expired":
                return AdmissionDecision(False, request_id, now, reason="expired")
            reason = None
            for blocked in result.blocked:
                if blocked["request_id"] == request_id:
                    reason = blocked.get("reason")
                    break
            return AdmissionDecision(False, request_id, now, reason=reason or "queued")
        return AdmissionDecision(False, request_id, now, reason="queued")

    submit = submit_request
    admit = submit_request
    request = submit_request
    create_request = submit_request

    def submit_now(self, *args: Any, **kwargs: Any) -> AdmissionDecision:
        """Submit and run one scheduling pass immediately."""
        kwargs["schedule"] = True
        return self.submit_request(*args, **kwargs)

    def enqueue_request(self, *args: Any, **kwargs: Any) -> AdmissionDecision:
        """Queue only; scheduling happens on the next ``tick``."""
        kwargs["schedule"] = False
        return self.submit_request(*args, **kwargs)

    acquire = submit_now

    def _new_permit_id(self) -> str:
        self._permit_seq += 1
        return f"permit-{self._permit_seq}"

    def _active_permits(self, endpoint: Optional[str] = None) -> list[dict[str, Any]]:
        result = []
        for permit in self.permits.values():
            if permit.get("terminal_at") is not None:
                continue
            if permit.get("state") in TERMINAL_STATES:
                continue
            if endpoint is not None and permit.get("endpoint") != endpoint:
                continue
            result.append(permit)
        return result

    def _hard_counts(self) -> dict[str, int]:
        counts = {name: 0 for name in self.endpoints}
        for permit in self._active_permits():
            ep = permit.get("endpoint")
            counts[ep] = counts.get(ep, 0) + 1
        return counts

    def _nonrecallable_counts(self) -> dict[str, int]:
        counts = {name: 0 for name in self.endpoints}
        for permit in self._active_permits():
            if permit.get("preemptible"):
                continue
            ep = permit.get("endpoint")
            counts[ep] = counts.get(ep, 0) + 1
        return counts

    def _capacity_maps(self) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
        hard = self._hard_counts()
        nonrecallable = self._nonrecallable_counts()
        free: dict[str, int] = {}
        potential: dict[str, int] = {}
        for name, ep in self.endpoints.items():
            cap = int(ep.get("current_cap", ep.get("cap", 0)))
            if not ep.get("connected", True):
                free[name] = 0
                potential[name] = 0
            else:
                free[name] = max(0, cap - hard.get(name, 0))
                potential[name] = max(0, cap - nonrecallable.get(name, 0))
        return hard, free, potential

    def _compatible_endpoints(self, skill: str) -> list[str]:
        return [name for name, ep in self.endpoints.items() if skill in ep.get("skills", set())]

    def _flow_for_ids(
        self,
        request_ids: Iterable[str],
        caps: dict[str, int],
    ) -> tuple[int, dict[str, str]]:
        ids = [rid for rid in dict.fromkeys(request_ids) if rid in self.requests]
        skills = [(rid, self.requests[rid]["skill"]) for rid in ids]
        return _max_flow_assignment(skills, caps, {
            name: ep.get("skills", set()) for name, ep in self.endpoints.items()
        })

    @staticmethod
    def _dec_caps(caps: dict[str, int], endpoint: str, amount: int = 1) -> dict[str, int]:
        copied = dict(caps)
        copied[endpoint] = max(0, int(copied.get(endpoint, 0)) - amount)
        return copied

    def _active_live_count(self, tenant: str) -> int:
        # Count request facts, not the tier snapshotted on an old permit, so a
        # class/guarantee configuration update cannot create two accounting views.
        return sum(
            1 for rid in self.queue
            if self.requests[rid]["tenant"] == tenant
            and self.requests[rid]["status"] in {"sent", "accepted", "running", "stopping"}
        )

    def _protected_ids(self, at: int) -> list[str]:
        protected: set[str] = set()
        for tenant_name, tenant in self.tenants.items():
            if tenant["class"] != "live":
                continue
            g = int(tenant.get("guarantee", 0))
            active = self._active_live_count(tenant_name)
            remaining_guarantee = max(0, g - active)
            delay = tenant.get("aging_delay", self.fairness_delay)
            tenant_queue = [
                rid for rid in self.queue
                if self.requests[rid]["tenant"] == tenant_name
                and self.requests[rid]["status"] == "queued"
            ]
            for index, rid in enumerate(tenant_queue):
                req = self.requests[rid]
                aged = delay is not None and at - int(req["submitted_at"]) >= int(delay)
                if index < remaining_guarantee or aged:
                    protected.add(rid)
        return [rid for rid in self.queue if rid in protected and self.requests[rid]["status"] == "queued"]

    def _aged_barriers(self, at: int) -> list[str]:
        result = []
        for rid in self.queue:
            req = self.requests[rid]
            if req["status"] != "queued" or req["tier"] != "live":
                continue
            tenant = self.tenants.get(req["tenant"], {})
            delay = tenant.get("aging_delay", self.fairness_delay)
            if delay is not None and at - int(req["submitted_at"]) >= int(delay):
                result.append(rid)
        return result

    def _fairness_blocker(self, rid: str, barriers: list[str]) -> Optional[str]:
        req = self.requests[rid]
        my_eps = set(self._compatible_endpoints(req["skill"]))
        if not my_eps:
            return None
        older = [bid for bid in barriers if self.requests[bid]["seq"] < req["seq"]]
        for bid in older:
            old_eps = set(self._compatible_endpoints(self.requests[bid]["skill"]))
            # If every endpoint this request can use is also in the older
            # barrier's compatibility scope, it cannot claim an independent
            # resource ahead of that older request.
            if my_eps and my_eps.issubset(old_eps):
                return bid
        return None

    def _evaluate_candidate(
        self,
        rid: str,
        endpoint: str,
        free: dict[str, int],
        potential: dict[str, int],
        protected: list[str],
        barriers: list[str],
        best_protected_flow: int,
    ) -> tuple[bool, Optional[str], Optional[str]]:
        req = self.requests[rid]
        if free.get(endpoint, 0) <= 0:
            return False, "no_free_slot", None

        older_barrier: Optional[str] = None
        for bid in barriers:
            if self.requests[bid]["seq"] >= req["seq"]:
                continue
            before, _ = self._flow_for_ids(barriers, free)
            after, _ = self._flow_for_ids(barriers, self._dec_caps(free, endpoint))
            if after < before:
                older_barrier = bid
                return False, "waiting_for_older_request", older_barrier

        residual_potential = self._dec_caps(potential, endpoint)
        if rid in protected:
            rest = [x for x in protected if x != rid]
            rest_flow, _ = self._flow_for_ids(rest, residual_potential)
            safe_count = 1 + rest_flow
        else:
            rest = list(protected)
            rest_flow, _ = self._flow_for_ids(rest, residual_potential)
            safe_count = rest_flow
        if safe_count < best_protected_flow:
            return False, "placement_conflict", None

        # New background borrowing must also not reduce the number of online
        # requests that can be placed against currently free slots.
        if req["tier"] == "batch" and protected:
            best_free, _ = self._flow_for_ids(protected, free)
            after_free, _ = self._flow_for_ids(protected, self._dec_caps(free, endpoint))
            if after_free < best_free:
                return False, "online_capacity_reserved", None
        return True, None, None

    def _choose_endpoint(
        self,
        rid: str,
        at: Optional[int] = None,
        context: Optional[dict[str, Any]] = None,
    ) -> tuple[Optional[str], Optional[str], Optional[str]]:
        if at is None:
            at = self.now
        req = self.requests[rid]
        deadline = req.get("deadline")
        if req.get("status") == "queued" and deadline is not None and at >= int(deadline):
            return None, "expired", None
        _, free, potential = self._capacity_maps()
        protected = self._protected_ids(at)
        barriers = self._aged_barriers(at)
        best_protected_flow, _ = self._flow_for_ids(protected, potential)
        candidates: list[tuple[Any, str, Optional[str]]] = []
        last_reason = "no_compatible_instance"
        last_fairness: Optional[str] = None

        for ep in self._compatible_endpoints(req["skill"]):
            ep_rec = self.endpoints[ep]
            if not ep_rec.get("connected", True):
                last_reason = "endpoint_disconnected"
                continue
            ok, reason, blocker = self._evaluate_candidate(
                rid, ep, free, potential, protected, barriers, best_protected_flow,
            )
            if ok:
                hard = self._hard_counts().get(ep, 0)
                # Prefer the most skill-specific instance, then the emptier one.
                candidates.append((len(ep_rec["skills"]), hard, ep))
            else:
                last_reason = reason or "unavailable"
                if blocker:
                    last_fairness = blocker

        if candidates:
            candidates.sort(key=lambda x: (x[0], x[1], x[2]))
            return candidates[0][2], None, None

        # Even when no slot exists, identify a same-scope aged barrier so
        # query output can distinguish queue fairness from physical obstruction.
        fairness_blocker = self._fairness_blocker(rid, barriers)
        if fairness_blocker:
            return None, "waiting_for_older_request", fairness_blocker
        return None, last_reason, last_fairness

    def _choose_lender(self) -> tuple[Optional[str], Optional[int]]:
        for name in sorted(self.tenants):
            tenant = self.tenants[name]
            if tenant["class"] != "live":
                continue
            g = int(tenant["guarantee"])
            active = self._active_live_count(name)
            if active < g:
                return name, active + 1
        return None, None

    def _admit(self, rid: str, endpoint: str, at: int) -> dict[str, Any]:
        req = self.requests[rid]
        pid = self._new_permit_id()
        lender, slot = (None, None)
        borrowed = False
        if req["tier"] == "batch":
            lender, slot = self._choose_lender()
            borrowed = lender is not None
        permit = {
            "permit_id": pid,
            "request_id": rid,
            "tenant": req["tenant"],
            "tier": req["tier"],
            "skill": req["skill"],
            "endpoint": endpoint,
            "epoch": self.endpoints[endpoint]["epoch"],
            "state": "sent",
            "outcome": None,
            "preemptible": bool(req["preemptible"]),
            "borrowed": borrowed,
            "lender_tenant": lender,
            "lender_slot": slot,
            "sent_at": at,
            "accepted_at": None,
            "terminal_at": None,
            "stop_requested": False,
            "stop_requested_at": None,
            "stop_emitted": False,
            "remote_unknown": False,
            "caller_gone": False,
            "history": [{"at": at, "event": "admitted"}],
        }
        self.permits[pid] = permit
        req["status"] = "sent"
        req["permit_id"] = pid
        req["endpoint"] = endpoint
        req["admitted_at"] = at
        req["history"].append({"at": at, "event": "sent", "permit": pid, "endpoint": endpoint})
        event = {
            "at": at,
            "kind": "sent",
            "record": {"id": rid, "permit": pid, "endpoint": endpoint, "epoch": permit["epoch"]},
        }
        self.outbox.append(event)
        self.events.append(event)
        return {
            "request_id": rid,
            "id": rid,
            "permit": pid,
            "endpoint": endpoint,
            "epoch": permit["epoch"],
            "at": at,
        }

    def admit_to(
        self,
        request_id: str,
        endpoint: str,
        at: Optional[int] = None,
    ) -> AdmissionDecision:
        """Try a specific compatible instance while preserving global safety."""
        now = self._set_time(at)
        req = self.requests.get(request_id)
        if req is None:
            raise AdmissionError(f"未知请求：{request_id}")
        if req["status"] != "queued":
            return AdmissionDecision(
                req["status"] in {"sent", "accepted", "running", "stopping", "done", "stopped"},
                request_id, now, permit_id=req.get("permit_id"),
                endpoint=req.get("endpoint"), reason=req["status"],
            )
        if endpoint not in self.endpoints:
            return AdmissionDecision(False, request_id, now, reason="unknown_endpoint")
        if req["skill"] not in self.endpoints[endpoint].get("skills", set()):
            return AdmissionDecision(False, request_id, now, reason="incompatible_endpoint")
        deadline = req.get("deadline")
        if deadline is not None and now >= int(deadline):
            req["status"] = "expired"
            return AdmissionDecision(False, request_id, now, reason="expired")

        _, free, potential = self._capacity_maps()
        protected = self._protected_ids(now)
        barriers = self._aged_barriers(now)
        best_flow, _ = self._flow_for_ids(protected, potential)
        ok, reason, blocker = self._evaluate_candidate(
            request_id, endpoint, free, potential, protected, barriers, best_flow,
        )
        if not ok:
            return AdmissionDecision(False, request_id, now, reason=reason, detail={"older_request_id": blocker})
        dispatch = self._admit(request_id, endpoint, now)
        return AdmissionDecision(
            True, request_id, now, permit_id=dispatch["permit"],
            endpoint=endpoint, reason="admitted",
        )

    def mark_sent(
        self,
        permit: Optional[str] = None,
        request_id: Optional[str] = None,
        endpoint: Optional[str] = None,
        epoch: int = 1,
        at: Optional[int] = None,
        record: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if isinstance(permit, dict) and record is None:
            record = permit
            permit = None
        if record is not None:
            permit = record.get("permit", permit)
            request_id = record.get("id") or record.get("request_id") or request_id
            endpoint = record.get("endpoint", endpoint)
            epoch = record.get("epoch", epoch)
            at = record.get("at", at)
        now = self._set_time(at)
        if not permit:
            raise AdmissionError("sent 缺少 permit")
        if not request_id or request_id not in self.requests:
            raise AdmissionError(f"sent 引用了未知请求：{request_id}")
        if not endpoint or endpoint not in self.endpoints:
            raise AdmissionError(f"sent 引用了未知实例：{endpoint}")
        req = self.requests[request_id]
        endpoint_rec = self.endpoints[endpoint]
        if req.get("skill") not in endpoint_rec.get("skills", set()):
            raise AdmissionError(
                f"请求 {request_id} 的 skill={req.get('skill')} 与实例 {endpoint} "
                f"的 skills={sorted(endpoint_rec.get('skills', set()))} 不兼容；"
                "不能把这种接纳计为保障生效"
            )

        req = self.requests[request_id]
        old_pid = req.get("permit_id")
        if old_pid and old_pid in self.permits and permit != old_pid:
            old_permit = self.permits[old_pid]
            old_active = old_permit.get("terminal_at") is None and old_permit.get("state") not in TERMINAL_STATES
            if old_active and old_permit.get("remote_unknown"):
                raise AdmissionError(
                    f"请求 {request_id} 在 {old_permit.get('endpoint')} 上仍有未明 permit "
                    f"{old_pid}；不能向同一物理位置重复发放 {permit}"
                )
        if permit in self.permits:
            p = self.permits[permit]
            if p["request_id"] != request_id:
                raise AdmissionError(f"permit {permit} 已属于 {p['request_id']}")
        elif old_pid and old_pid in self.permits:
            p = self.permits.pop(old_pid)
            p["permit_id"] = permit
            self.permits[permit] = p
        else:
            p = {
                "permit_id": permit,
                "request_id": request_id,
                "tenant": req["tenant"],
                "tier": req["tier"],
                "skill": req["skill"],
                "state": "sent",
                "outcome": None,
                "preemptible": bool(req["preemptible"]),
                "borrowed": req["tier"] == "batch",
                "lender_tenant": None,
                "lender_slot": None,
                "accepted_at": None,
                "terminal_at": None,
                "stop_requested": False,
                "stop_requested_at": None,
                "stop_emitted": False,
                "remote_unknown": False,
                "caller_gone": False,
                "history": [],
            }
            self.permits[permit] = p

        p["endpoint"] = endpoint
        p["epoch"] = int(epoch)
        p.setdefault("sent_at", now)
        if p.get("terminal_at") is None and p.get("state") not in {"accepted", "running", "stopping"}:
            p["state"] = "sent"
        p["history"].append({"at": now, "event": "sent_confirmation"})
        req["status"] = "sent" if req["status"] == "queued" else req["status"]
        req["permit_id"] = permit
        req["endpoint"] = endpoint
        req["admitted_at"] = req.get("admitted_at") or now
        return p

    sent = mark_sent

    def accept(self, permit: str | dict[str, Any], at: Optional[int] = None, record: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        if isinstance(permit, dict) and record is None:
            record = permit
            permit = record.get("permit")
        if record is not None:
            permit = record.get("permit", permit)
            at = record.get("at", at)
        now = self._set_time(at)
        p = self.permits.get(permit)
        if not p:
            raise AdmissionError(f"accept 引用未知 permit：{permit}")
        if p.get("terminal_at") is not None:
            raise AdmissionError(f"permit {permit} 已结束，不能 accept")
        p["state"] = "accepted"
        p["accepted_at"] = now
        p["remote_unknown"] = False
        p["history"].append({"at": now, "event": "accepted"})
        req = self.requests[p["request_id"]]
        if req["status"] not in TERMINAL_STATES:
            req["status"] = "accepted"
        req["accepted_at"] = now
        return p

    adopt = accept
    accept_permit = accept
    handle_accept = accept
    on_accept = accept

    def request_stop(
        self,
        permit: str | dict[str, Any],
        at: Optional[int] = None,
        reason: str = "stop_requested",
        record: Optional[dict[str, Any]] = None,
    ) -> Optional[dict[str, Any]]:
        if isinstance(permit, dict) and record is None:
            record = permit
            permit = record.get("permit")
        if record is not None:
            permit = record.get("permit", permit)
            at = record.get("at", at)
        now = self._set_time(at)
        p = self.permits.get(permit)
        if not p:
            raise AdmissionError(f"stop 引用未知 permit：{permit}")
        if p.get("terminal_at") is not None:
            return None
        if p.get("stop_requested"):
            return None
        p["stop_requested"] = True
        p["stop_requested_at"] = now
        p["stop_emitted"] = True
        p["state"] = "stopping"
        p["history"].append({"at": now, "event": "stop_requested", "reason": reason})
        event = {"at": now, "kind": "stop", "record": {"permit": permit}, "reason": reason}
        self.outbox.append(event)
        self.events.append(event)
        return {"permit": permit, "request_id": p["request_id"], "endpoint": p["endpoint"], "at": now}

    stop = request_stop
    stop_permit = request_stop
    report_stop = request_stop
    handle_stop = request_stop

    def report_result(
        self,
        permit: Optional[str] = None,
        outcome: Optional[str] = None,
        endpoint: Optional[str] = None,
        epoch: Optional[int] = None,
        at: Optional[int] = None,
        record: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if isinstance(permit, dict) and record is None:
            record = permit
            permit = None
        if record is not None:
            permit = record.get("permit", permit)
            outcome = record.get("outcome", outcome)
            endpoint = record.get("endpoint", endpoint)
            epoch = record.get("epoch", epoch)
            at = record.get("at", at)
        now = self._set_time(at)
        p = self.permits.get(permit or "")
        if not p:
            raise AdmissionError(f"result 引用未知 permit：{permit}")
        if outcome not in {"running", "stopped", "done", "unknown"}:
            raise AdmissionError(f"未知 result outcome：{outcome}")
        if endpoint is not None and p.get("endpoint") != endpoint:
            raise AdmissionError(
                f"result endpoint 不匹配：{permit} 属于 {p.get('endpoint')}，收到 {endpoint}"
            )
        if epoch is not None and int(epoch) != int(p.get("epoch", epoch)):
            raise AdmissionError(
                f"result epoch 不匹配：{permit} 属于 epoch {p.get('epoch')}，收到 {epoch}"
            )

        p["outcome"] = outcome
        p["history"].append({"at": now, "event": "result", "outcome": outcome})
        req = self.requests[p["request_id"]]

        if outcome == "unknown":
            # Deliberately not terminal: the slot remains charged to this
            # physical endpoint until done/stopped arrives.
            p["state"] = "unknown"
            p["remote_unknown"] = True
            p["unknown_since"] = now
            if record and record.get("connection") == "disconnected":
                self.endpoints[p["endpoint"]]["connected"] = False
        elif outcome == "running":
            p["state"] = "running"
            p["remote_unknown"] = False
        elif outcome in DEFINITE_RESULTS:
            p["state"] = outcome
            p["terminal_at"] = now
            p["remote_unknown"] = False
            req["status"] = outcome
            req["completed_at"] = now
        return p

    def complete(
        self,
        permit: Optional[str] = None,
        outcome: str = "done",
        endpoint: Optional[str] = None,
        epoch: Optional[int] = None,
        at: Optional[int] = None,
        record: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return self.report_result(
            permit=permit,
            outcome=outcome or "done",
            endpoint=endpoint,
            epoch=epoch,
            at=at,
            record=record,
            **kwargs,
        )

    finish = complete
    report_completion = complete
    on_complete = complete
    handle_result = report_result
    on_result = report_result
    report_remote_result = report_result

    def snapshot(self) -> dict[str, Any]:
        """Return an in-process durable-state snapshot."""
        import copy

        return copy.deepcopy({
            "now": self.now,
            "fairness_delay": self.fairness_delay,
            "tenants": self.tenants,
            "endpoints": self.endpoints,
            "requests": self.requests,
            "permits": self.permits,
            "queue": self.queue,
            "restarts": self.restarts,
            "request_seq": self._request_seq,
            "permit_seq": self._permit_seq,
        })

    def restore_snapshot(self, state: dict[str, Any]) -> "AdmissionService":
        import copy

        state = copy.deepcopy(state)
        self.now = int(state.get("now", 0))
        self.fairness_delay = state.get("fairness_delay", self.fairness_delay)
        self.tenants = state.get("tenants", {})
        self.endpoints = state.get("endpoints", {})
        self.requests = state.get("requests", {})
        self.permits = state.get("permits", {})
        self.queue = list(state.get("queue", []))
        self.restarts = int(state.get("restarts", 0))
        self._request_seq = int(state.get("request_seq", 0))
        self._permit_seq = int(state.get("permit_seq", 0))
        self.last_result = None
        return self

    @classmethod
    def from_snapshot(cls, state: dict[str, Any]) -> "AdmissionService":
        service = cls(now=state.get("now", 0), fairness_delay=state.get("fairness_delay", 3))
        return service.restore_snapshot(state)

    def restart(self, at: Optional[int] = None, record: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        now = self._set_time(at)
        self.restarts += 1
        self.events.append({"at": now, "kind": "restart", "record": record or {}})
        # Authoritative dictionaries survive the logical process restart.
        # A production deployment would rebuild these from the durable journal.
        return {"restarts": self.restarts, "at": now, "active_permits": len(self._active_permits())}

    # ------------------------------------------------------------------
    # Scheduling
    # ------------------------------------------------------------------
    def tick(self, at: Optional[int] = None) -> TickResult:
        now = self._set_time(at)
        result = TickResult(at=now)
        self._expire_due(now, result)
        self._restore_derates(now)

        protected = self._protected_ids(now)
        ordered = self._scheduling_order(protected)
        for rid in ordered:
            req = self.requests[rid]
            if req["status"] != "queued":
                continue
            endpoint, reason, fairness_blocker = self._choose_endpoint(rid, now)
            if endpoint is not None:
                dispatch = self._admit(rid, endpoint, now)
                result.dispatches.append(dispatch)
            else:
                explanation = self._explain_wait(rid, now)
                item = {
                    "request_id": rid,
                    "tenant": req["tenant"],
                    "skill": req["skill"],
                    "reason": reason or explanation["reason"],
                    "endpoint": None,
                    "blockers": explanation["blockers"],
                }
                result.blocked.append(item)
                if reason == "waiting_for_older_request" or fairness_blocker:
                    fb = dict(item)
                    fb["older_request_id"] = fairness_blocker
                    fb["reason"] = "waiting_for_older_request"
                    result.blocked_for_fairness.append(fb)

        # Reclaim compatible borrowed slots for still-unfulfilled guarantees.
        for rid in self._protected_ids(now):
            req = self.requests[rid]
            if req["status"] != "queued":
                continue
            stop = self._emit_recall(rid, now)
            if stop:
                result.stops.append(stop)

        self.last_result = result
        return result

    run_tick = tick
    schedule_once = tick

    def _expire_due(self, now: int, result: TickResult) -> None:
        for rid in self.queue:
            req = self.requests[rid]
            deadline = req.get("deadline")
            if deadline is None:
                continue
            if req["status"] == "queued" and now >= int(deadline):
                req["status"] = "expired"
                req["expired_at"] = now
                req["history"].append({"at": now, "event": "expired_before_admission"})
                result.expired.append(rid)
            elif req.get("permit_id") and now >= int(deadline):
                # The caller may stop waiting, but the execution fact and its
                # physical slot remain until a definite remote result.
                permit = self.permits.get(req["permit_id"])
                if permit and permit.get("terminal_at") is None:
                    permit["caller_gone"] = True
                    req["caller_gone"] = True

    def _restore_derates(self, now: int) -> None:
        for ep in self.endpoints.values():
            until = ep.get("cap_until")
            if until is not None and now >= int(until) and ep.get("normal_cap") is not None:
                ep["cap"] = int(ep["normal_cap"])
                ep["current_cap"] = int(ep["normal_cap"])
                ep["cap_until"] = None

    def _scheduling_order(self, protected: list[str]) -> list[str]:
        pset = set(protected)
        queued = [rid for rid in self.queue if self.requests[rid]["status"] == "queued"]
        return sorted(
            queued,
            key=lambda rid: (
                0 if self.requests[rid]["tier"] == "live" else 1,
                0 if rid in pset else 1,
                self.requests[rid]["seq"],
            ),
        )

    def _emit_recall(self, rid: str, at: int) -> Optional[dict[str, Any]]:
        req = self.requests[rid]
        _, free, _ = self._capacity_maps()
        endpoints = self._compatible_endpoints(req["skill"])
        endpoints.sort(key=lambda e: (len(self.endpoints[e]["skills"]), e))
        for ep_name in endpoints:
            if free.get(ep_name, 0) > 0:
                # A free slot exists but placement safety rejected it.  Do not
                # manufacture a recall unless the endpoint is actually full.
                continue
            occupants = self._active_permits(ep_name)
            # Prefer an un-stopped pausable borrower.
            candidates = [
                p for p in occupants
                if p.get("preemptible") and not p.get("stop_requested")
            ]
            candidates.sort(key=lambda p: (not p.get("borrowed", False), p["permit_id"]))
            if candidates:
                return self.request_stop(
                    candidates[0]["permit_id"], at=at, reason=f"guarantee_for_{rid}"
                )
        return None

    # ------------------------------------------------------------------
    # Explanations and queries
    # ------------------------------------------------------------------
    def _blocker_view(self, permit: dict[str, Any]) -> dict[str, Any]:
        req = self.requests.get(permit["request_id"], {})
        if permit.get("remote_unknown") or permit.get("state") == "unknown":
            kind = "remote_unknown"
            zh = "远端结果未明，仍计入该物理实例占用"
        elif not permit.get("preemptible", False):
            kind = "nonpreemptible_occupant"
            zh = "该调用声明不可暂停，只能等待其真实完成"
        elif permit.get("stop_requested") or permit.get("state") == "stopping":
            kind = "waiting_stop_confirmation"
            zh = "已要求停止，但尚未收到 stopped/done 确认"
        else:
            kind = "preemptible_occupant"
            zh = "可暂停借用尚未收回"
        return {
            "type": kind,
            "reason": kind,
            "reason_zh": zh,
            "permit": permit["permit_id"],
            "request_id": permit["request_id"],
            "tenant": permit.get("tenant"),
            "endpoint": permit.get("endpoint"),
            "epoch": permit.get("epoch"),
            "state": permit.get("state"),
            "outcome": permit.get("outcome"),
            "preemptible": permit.get("preemptible"),
            "stop_requested": permit.get("stop_requested"),
            "remote_unknown": permit.get("remote_unknown"),
            "submitted_at": req.get("submitted_at"),
        }

    def _explain_wait(self, rid: str, at: Optional[int] = None) -> dict[str, Any]:
        at = self._set_time(at)
        req = self.requests[rid]
        compatible = self._compatible_endpoints(req["skill"])
        endpoint_views = {name: self.inspect_endpoint(name) for name in self.endpoints}
        blockers: list[dict[str, Any]] = []
        for ep in compatible:
            for p in self._active_permits(ep):
                blockers.append(self._blocker_view(p))

        endpoint_reasons = []
        for ep in compatible:
            ep_rec = self.endpoints[ep]
            if not ep_rec.get("connected", True):
                endpoint_reasons.append("endpoint_disconnected")
            elif int(ep_rec.get("current_cap", ep_rec["cap"])) == 0:
                endpoint_reasons.append("endpoint_at_zero_capacity")

        endpoint, candidate_reason, fairness_blocker = self._choose_endpoint(rid, at)
        admittable = endpoint is not None
        if admittable:
            reason = "ready_to_admit"
            reason_zh = "现在有安全兼容位置，可继续准入"
        elif req["status"] == "expired" or candidate_reason == "expired" or (
            req.get("deadline") is not None and at >= int(req["deadline"])
        ):
            reason = "expired"
            reason_zh = "未获准即已超过等待期限，不再调度"
        elif fairness_blocker or candidate_reason == "waiting_for_older_request":
            reason = "waiting_for_older_request"
            reason_zh = "更早且已连续等待的在线请求优先；独立兼容资源上的新请求仍可继续"
        elif any(b["reason"] == "remote_unknown" for b in blockers):
            reason = "remote_unknown"
            reason_zh = "远端连接/结果未明，占用不能核销，也不能向同一位置重复发放"
        elif any(b["reason"] == "waiting_stop_confirmation" for b in blockers):
            reason = "waiting_for_stop_confirmation"
            reason_zh = "停止请求尚未得到实例确认，必须等待真实结束"
        elif any(b["reason"] == "nonpreemptible_occupant" for b in blockers):
            reason = "nonpreemptible_occupant"
            reason_zh = "不可暂停的活动仍在执行，保底需等待其完成"
        elif not compatible:
            reason = "no_compatible_instance"
            reason_zh = "没有任何实例声明支持该 skill"
        elif "endpoint_at_zero_capacity" in endpoint_reasons and all(
            int(self.endpoints[e].get("current_cap", self.endpoints[e]["cap"])) == 0
            for e in compatible
        ):
            reason = "endpoint_at_zero_capacity"
            reason_zh = "兼容实例当前限额为 0；已有执行事实不撤销，但不能新发放"
        elif "endpoint_disconnected" in endpoint_reasons and all(
            not self.endpoints[e].get("connected", True) for e in compatible
        ):
            reason = "endpoint_disconnected"
            reason_zh = "兼容实例连接断开，未明占用仍归属该物理实例"
        elif candidate_reason == "placement_conflict":
            reason = "placement_conflict"
            reason_zh = "若选择该位置，会使其它已排队在线请求没有实际兼容位置"
        else:
            reason = candidate_reason or "no_available_capacity"
            reason_zh = "兼容实例当前没有安全可用名额"

        return {
            "request_id": rid,
            "status": req["status"],
            "reason": reason,
            "reason_zh": reason_zh,
            "admittable_now": admittable,
            "candidate_endpoint": endpoint,
            "fairness_blocker": fairness_blocker,
            "usable_endpoints": compatible,
            "blockers": blockers,
            "deadline": req.get("deadline"),
            "remaining_wait": None if req.get("deadline") is None else req["deadline"] - at,
            "endpoint_states": {e: endpoint_views[e] for e in compatible},
        }

    def inspect_tenant(self, name: str, at: Optional[int] = None) -> dict[str, Any]:
        now = self._set_time(at)
        tenant = self.tenants.get(name)
        if not tenant:
            raise AdmissionError(f"未知租户：{name}")

        tenant_requests = [self.requests[rid] for rid in self.queue if self.requests[rid]["tenant"] == name]
        active_requests = [r for r in tenant_requests if r["status"] in {"sent", "accepted", "running", "stopping"}]
        queued_requests = [r for r in tenant_requests if r["status"] == "queued"]
        g = int(tenant["guarantee"])

        slots: list[dict[str, Any]] = []
        shown_permit_ids: set[str] = set()
        active_sorted = sorted(active_requests, key=lambda r: (r.get("admitted_at") or 0, r["seq"]))
        for i in range(g):
            if i < len(active_sorted):
                r = active_sorted[i]
                if r.get("permit_id"):
                    shown_permit_ids.add(r["permit_id"])
                p = self.permits.get(r["permit_id"]) if r.get("permit_id") else None
                slots.append({
                    "index": i + 1,
                    "state": "effective",
                    "effective": True,
                    "reason": "execution_fact",
                    "reason_zh": "已有获准/接纳的执行事实",
                    "request_id": r["id"],
                    "permit": r.get("permit_id"),
                    "endpoint": r.get("endpoint"),
                    "admitted_at": r.get("admitted_at"),
                    "accepted_at": r.get("accepted_at"),
                    "true_entry_at": r.get("accepted_at") or r.get("admitted_at"),
                    "accepted": bool(p and p.get("accepted_at") is not None),
                    "remote_unknown": bool(p and p.get("remote_unknown")),
                })
            else:
                q_index = i - min(g, len(active_sorted))
                guaranteed_queue = queued_requests[:max(0, g - len(active_sorted))]
                if q_index < len(guaranteed_queue):
                    rid = guaranteed_queue[q_index]["id"]
                    explanation = self._explain_wait(rid, now)
                    for blocker in explanation.get("blockers", []):
                        if blocker.get("permit"):
                            shown_permit_ids.add(blocker["permit"])
                    slots.append({
                        "index": i + 1,
                        "state": "pending",
                        "effective": False,
                        "request_id": rid,
                        **{k: explanation[k] for k in (
                            "reason", "reason_zh", "blockers", "usable_endpoints",
                            "admittable_now", "candidate_endpoint", "fairness_blocker",
                            "remaining_wait",
                        )},
                    })
                else:
                    lent = self._find_lent_permit(name, i + 1, exclude=shown_permit_ids)
                    if lent:
                        shown_permit_ids.add(lent["permit_id"])
                        slots.append({
                            "index": i + 1,
                            "state": "lent",
                            "effective": False,
                            "reason": "lent_to_background",
                            "reason_zh": "保底空闲时已临时借给后台；需要时会从兼容实例收回",
                            "borrower_request_id": lent["request_id"],
                            "permit": lent["permit_id"],
                            "endpoint": lent["endpoint"],
                            "preemptible": lent.get("preemptible"),
                            "stop_requested": lent.get("stop_requested"),
                        })
                    else:
                        slots.append({
                            "index": i + 1,
                            "state": "idle_available",
                            "effective": None,
                            "reason": "idle_no_demand",
                            "reason_zh": "当前没有需要该保底名额的在线请求",
                        })

        explanations = [self._explain_wait(r["id"], now) for r in queued_requests]
        admittable = [e["request_id"] for e in explanations if e["admittable_now"]]
        continuable = [
            {
                "request_id": e["request_id"],
                "endpoint": e["candidate_endpoint"],
                "reason": e["reason"],
                "reason_zh": e["reason_zh"],
            }
            for e in explanations
            if e["admittable_now"]
        ]
        blocked_requests = [
            {
                "request_id": e["request_id"],
                "reason": e["reason"],
                "reason_zh": e["reason_zh"],
                "blockers": e["blockers"],
                "fairness_blocker": e["fairness_blocker"],
            }
            for e in explanations
            if not e["admittable_now"]
        ]

        return {
            "at": now,
            "tenant": name,
            "class": tenant["class"],
            "tier": tenant["tier"],
            "guarantee": g,
            "effective_guarantee": min(g, len(active_requests)),
            "active_online": len(active_requests) if tenant["class"] == "live" else 0,
            "queued_online": len(queued_requests) if tenant["class"] == "live" else 0,
            "guarantee_slots": slots,
            "waiting_requests": explanations,
            "admittable_now": admittable,
            "continuable_requests": continuable,
            "blocked_requests": blocked_requests,
            "endpoints": {name: self.inspect_endpoint(name) for name in self.endpoints},
        }

    def _find_lent_permit(
        self,
        tenant: str,
        slot: int,
        exclude: Optional[set[str]] = None,
    ) -> Optional[dict[str, Any]]:
        excluded = exclude or set()
        for p in self._active_permits():
            if p["permit_id"] in excluded:
                continue
            if p.get("lender_tenant") == tenant and p.get("lender_slot") == slot:
                return p
        # If configuration history moved slot numbering, show each remaining
        # active background borrow at most once.
        for p in self._active_permits():
            if p["permit_id"] in excluded:
                continue
            if p.get("borrowed") and p.get("tier") == "batch":
                return p
        return None

    def inspect_endpoint(self, name: str, at: Optional[int] = None) -> dict[str, Any]:
        if at is not None:
            self._set_time(at)
        ep = self.endpoints.get(name)
        if not ep:
            raise AdmissionError(f"未知实例：{name}")
        active = self._active_permits(name)
        hard = len(active)
        cap = int(ep.get("current_cap", ep["cap"]))
        return {
            "at": self.now,
            "name": name,
            "epoch": ep["epoch"],
            "skills": sorted(ep.get("skills", set())),
            "cap": ep["cap"],
            "current_cap": cap,
            "connected": ep.get("connected", True),
            "active": hard,
            "hard": hard,
            "available": max(0, cap - hard),
            "over_capacity": hard > cap,
            "unknown": sum(1 for p in active if p.get("remote_unknown")),
            "stopping": sum(1 for p in active if p.get("stop_requested")),
            "permits": [
                {
                    "permit": p["permit_id"],
                    "request_id": p["request_id"],
                    "tenant": p.get("tenant"),
                    "state": p.get("state"),
                    "outcome": p.get("outcome"),
                    "preemptible": p.get("preemptible"),
                    "borrowed": p.get("borrowed"),
                    "remote_unknown": p.get("remote_unknown"),
                    "stop_requested": p.get("stop_requested"),
                }
                for p in active
            ],
        }

    def inspect(self, target: Optional[str | dict[str, Any]] = None, **kwargs: Any) -> dict[str, Any]:
        if isinstance(target, dict):
            kwargs.update(target)
            target = None
        tenant = kwargs.get("tenant")
        endpoint = kwargs.get("endpoint")
        if tenant:
            return self.inspect_tenant(tenant)
        if endpoint:
            return self.inspect_endpoint(endpoint)
        if target in self.tenants:
            return self.inspect_tenant(target)
        if target in self.endpoints:
            return self.inspect_endpoint(target)
        return {
            "at": self.now,
            "guarantees": self.guarantee_report(),
            "tenants": {n: self.inspect_tenant(n) for n in self.tenants},
            "endpoints": {n: self.inspect_endpoint(n) for n in self.endpoints},
            "continuations": self.continuations(),
            "queue": list(self.queue),
        }

    query = inspect
    status = inspect
    describe = inspect
    dump = inspect
    query_tenant = inspect_tenant
    query_endpoint = inspect_endpoint
    tenant_view = inspect_tenant
    endpoint_view = inspect_endpoint

    def guarantee_report(self, at: Optional[int] = None) -> list[dict[str, Any]]:
        """Flatten every configured guarantee slot with its explanation."""
        result = []
        for name in sorted(self.tenants):
            if self.tenants[name]["class"] != "live":
                continue
            view = self.inspect_tenant(name, at=at)
            for slot in view["guarantee_slots"]:
                item = {"tenant": name, "guarantee": view["guarantee"]}
                item.update(slot)
                result.append(item)
        return result

    def why_guarantee_not_effective(self, at: Optional[int] = None) -> list[dict[str, Any]]:
        return [slot for slot in self.guarantee_report(at) if slot.get("effective") is False]

    def continuations(self, at: Optional[int] = None) -> list[dict[str, Any]]:
        """All queued calls that can safely receive a dispatch right now."""
        if at is not None:
            self._set_time(at)
        result = []
        for rid in self.queue:
            if self.requests[rid]["status"] != "queued":
                continue
            explanation = self._explain_wait(rid, self.now)
            if explanation["admittable_now"]:
                result.append({
                    "request_id": rid,
                    "tenant": self.requests[rid]["tenant"],
                    "skill": self.requests[rid]["skill"],
                    "endpoint": explanation["candidate_endpoint"],
                    "reason": explanation["reason"],
                })
        return result

    what_can_continue = continuations
    can_continue = continuations
    next_admissions = continuations

    # ------------------------------------------------------------------
    # JSONL message entry
    # ------------------------------------------------------------------
    def handle_message(self, row: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(row, dict) or "kind" not in row or "record" not in row:
            raise AdmissionError("消息需要 at、kind 和 record 字段")
        at = int(row.get("at", self.now))
        kind = row["kind"]
        record = row.get("record") or {}
        self._set_time(at)

        if kind == "tenant":
            view = self.load_tenant(record, at=at)
            return {"ok": True, "kind": kind, "at": at, "tenant": view["name"]}
        if kind == "endpoint":
            view = self.load_endpoint(record, at=at)
            return {"ok": True, "kind": kind, "at": at, "endpoint": view["name"]}
        if kind == "request":
            decision = self.submit_request(record=record, at=at, schedule=False)
            return {"ok": True, "kind": kind, "at": at, "decision": decision.to_dict()}
        if kind == "sent":
            p = self.mark_sent(record=record, at=at)
            return {"ok": True, "kind": kind, "at": at, "permit": p["permit_id"]}
        if kind == "accept":
            p = self.accept(record.get("permit"), at=at)
            return {"ok": True, "kind": kind, "at": at, "permit": p["permit_id"]}
        if kind == "stop":
            stop = self.request_stop(record.get("permit"), at=at, record=record)
            return {"ok": True, "kind": kind, "at": at, "stop": stop}
        if kind == "result":
            p = self.report_result(record=record, at=at)
            return {"ok": True, "kind": kind, "at": at, "permit": p["permit_id"], "outcome": p["outcome"]}
        if kind == "tick":
            result = self.tick(at=at)
            return {"ok": True, "kind": kind, "at": at, "tick": result.to_dict()}
        if kind == "restart":
            info = self.restart(at=at, record=record)
            return {"ok": True, "kind": kind, "at": at, "restart": info}
        if kind == "inspect":
            if record.get("tenant"):
                view = self.inspect_tenant(record["tenant"], at=at)
                return {"ok": True, "kind": kind, "at": at, "view": view}
            if record.get("endpoint"):
                view = self.inspect_endpoint(record["endpoint"], at=at)
                return {"ok": True, "kind": kind, "at": at, "view": view}
            view = self.inspect(at=at)
            return {"ok": True, "kind": kind, "at": at, "view": view}
        raise AdmissionError(f"未登记的消息名称：{kind}")


class LocalExecutionSide:
    """Small adapter showing how a real local executor should call back."""

    def __init__(self, service: AdmissionService) -> None:
        self.service = service

    def adopt(self, permit: str, at: Optional[int] = None) -> dict[str, Any]:
        return self.service.accept(permit, at=at)

    def complete(self, permit: str, at: Optional[int] = None) -> dict[str, Any]:
        return self.service.report_result(permit, "done", at=at)

    def confirm_stopped(self, permit: str, at: Optional[int] = None) -> dict[str, Any]:
        return self.service.report_result(permit, "stopped", at=at)

    def report_unknown(self, permit: str, at: Optional[int] = None) -> dict[str, Any]:
        return self.service.report_result(permit, "unknown", at=at)

    def stop_requested(self, permit: str, at: Optional[int] = None) -> dict[str, Any]:
        """Executor-side behavior after receiving a stop command.

        Pausable work confirms ``stopped`` immediately in this deterministic
        adapter.  Non-pausable work reports ``running`` and later must be
        completed naturally by calling :meth:`complete`.
        """
        p = self.service.permits[permit]
        if p.get("preemptible"):
            return self.confirm_stopped(permit, at=at)
        return self.service.report_result(permit, "running", at=at)


@dataclass
class ReplayResult:
    path: str
    service: AdmissionService
    responses: list[dict[str, Any]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    views: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    view_history: dict[tuple[str, str], list[dict[str, Any]]] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.errors

    def final_request(self, request_id: str) -> dict[str, Any]:
        return self.service.requests[request_id]

    def views_at_or_before(self, kind: str, name: str, at: int) -> list[dict[str, Any]]:
        key = (kind, name)
        return [v for v in self.view_history.get(key, []) if int(v.get("at", 0)) <= at]


def replay_jsonl(path: str | Path, fairness_delay: int = 3) -> ReplayResult:
    path = Path(path)
    svc = AdmissionService(fairness_delay=fairness_delay)
    result = ReplayResult(path=str(path), service=svc)
    with path.open("r", encoding="utf-8") as source:
        for line_number, raw in enumerate(source, 1):
            if not raw.strip():
                continue
            try:
                row = json.loads(raw)
                response = svc.handle_message(row)
                result.responses.append(response)
                if row.get("kind") == "inspect" and response.get("view"):
                    rec = row.get("record", {})
                    if rec.get("tenant"):
                        key = ("tenant", rec["tenant"])
                    elif rec.get("endpoint"):
                        key = ("endpoint", rec["endpoint"])
                    else:
                        key = ("all", "all")
                    view = response["view"]
                    result.views[key] = view
                    result.view_history.setdefault(key, []).append(view)
            except Exception as exc:  # Include line context for drill files.
                result.errors.append(f"{path.name} 第 {line_number} 行：{exc}")
    return result


TrafficAdmissionService = AdmissionService
TrafficAdmission = AdmissionService
Admission = AdmissionService
Service = AdmissionService
create_service = AdmissionService


def replay_messages(rows: Iterable[dict[str, Any]], fairness_delay: int = 3) -> AdmissionService:
    """Rebuild a service from in-memory JSONL-style messages."""
    svc = AdmissionService(fairness_delay=fairness_delay)
    for row in rows:
        svc.handle_message(row)
    return svc


load_messages = replay_messages
run_messages = replay_messages


def load_scenario(path: str | Path, fairness_delay: int = 3) -> ReplayResult:
    return replay_jsonl(path, fairness_delay=fairness_delay)


run_scenario = replay_jsonl


def _json_default(value: Any) -> Any:
    if isinstance(value, set):
        return sorted(value)
    if hasattr(value, "to_dict"):
        return value.to_dict()
    return str(value)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="回放工作台准入 JSONL 演练资料")
    parser.add_argument("paths", nargs="+", type=Path, help="JSONL 文件或包含 JSONL 的目录")
    parser.add_argument("--quiet-responses", action="store_true", help="只输出 inspect 视图和错误")
    args = parser.parse_args(argv)

    files: list[Path] = []
    for raw_path in args.paths:
        if raw_path.is_dir():
            files.extend(sorted(raw_path.glob("*.jsonl")))
        else:
            files.append(raw_path)

    outputs = []
    for path in files:
        result = replay_jsonl(path)
        item: dict[str, Any] = {
            "file": str(path),
            "ok": result.ok,
            "errors": result.errors,
            "inspections": [
                {"target": list(key), "view": view}
                for key, view in result.views.items()
            ],
        }
        if not args.quiet_responses:
            item["responses"] = result.responses
        outputs.append(item)
    print(json.dumps(outputs, ensure_ascii=False, indent=2, default=_json_default))
    return 0 if all(item["ok"] for item in outputs) else 1


if __name__ == "__main__":
    raise SystemExit(main())
