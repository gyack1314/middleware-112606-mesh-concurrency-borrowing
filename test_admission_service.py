import json
import tempfile
import unittest
from pathlib import Path

from admission_service import AdmissionError, AdmissionService, replay_jsonl


class BorrowRecallTests(unittest.TestCase):
    def setUp(self):
        self.svc = AdmissionService(fairness_delay=3)
        self.svc.upsert_tenant("live", tier="live", guarantee=1, at=0)
        self.svc.upsert_tenant("history", tier="batch", guarantee=0, at=0)
        self.svc.upsert_endpoint("a", skills=["render"], cap=1, epoch=1, at=0)
        self.svc.upsert_endpoint("b", skills=["index"], cap=1, epoch=1, at=0)

    def test_unknown_result_keeps_physical_slot_until_definite_end(self):
        d = self.svc.submit_request(
            id="back", tenant="history", skill="render",
            deadline=20, preemptible=True, at=1, schedule=False,
        )
        self.assertFalse(d.admitted)
        out = self.svc.tick(at=1)
        self.assertEqual([x["endpoint"] for x in out.dispatches], ["a"])
        permit = out.dispatches[0]["permit"]

        self.svc.mark_sent(permit=permit, request_id="back", endpoint="a", epoch=1, at=2)
        self.svc.accept(permit, at=3)

        self.svc.submit_request(
            id="live1", tenant="live", skill="render",
            deadline=16, preemptible=False, at=4, schedule=False,
        )
        out = self.svc.tick(at=4)
        self.assertEqual(out.dispatches, [])
        self.assertEqual([s["permit"] for s in out.stops], [permit])

        self.svc.report_result(permit, endpoint="a", epoch=1, outcome="unknown", at=6)
        self.svc.tick(at=7)
        view = self.svc.inspect_tenant("live")
        slot = view["guarantee_slots"][0]
        self.assertFalse(slot["effective"])
        self.assertEqual(slot["reason"], "remote_unknown")
        self.assertEqual(slot["blockers"][0]["permit"], permit)
        self.assertEqual(slot["blockers"][0]["endpoint"], "a")
        self.assertNotIn("live1", view["admittable_now"])

        # Unknown is still charged: no second permit can use instance a.
        self.svc.submit_request(
            id="render2", tenant="history", skill="render", deadline=30,
            preemptible=True, at=7, schedule=False,
        )
        self.svc.tick(at=7)
        ep = self.svc.inspect_endpoint("a")
        self.assertEqual(ep["active"], 1)
        self.assertEqual(ep["available"], 0)
        self.assertEqual([x["request_id"] for x in self.svc.last_result.dispatches], [])

        self.svc.report_result(permit, endpoint="a", epoch=1, outcome="stopped", at=11)
        out = self.svc.tick(at=11)
        self.assertEqual([x["request_id"] for x in out.dispatches], ["live1"])
        self.assertEqual(out.dispatches[0]["endpoint"], "a")

    def test_nonpreemptible_borrower_is_actual_blocker(self):
        out = self.svc.submit_request(
            id="slow", tenant="history", skill="render", deadline=30,
            preemptible=False, at=1, schedule=True,
        )
        permit = out.permit_id
        self.svc.accept(permit, at=2)

        self.svc.submit_request(
            id="live1", tenant="live", skill="render", deadline=16, at=4,
        )
        view = self.svc.inspect_tenant("live")
        slot = view["guarantee_slots"][0]
        self.assertFalse(slot["effective"])
        self.assertEqual(slot["reason"], "nonpreemptible_occupant")
        self.assertEqual(slot["blockers"][0]["permit"], permit)
        self.assertEqual(self.svc.tick(at=5).stops, [])

        self.svc.report_result(permit, outcome="done", endpoint="a", epoch=1, at=8)
        out = self.svc.tick(at=8)
        self.assertEqual([x["request_id"] for x in out.dispatches], ["live1"])


class PlacementAndCapacityTests(unittest.TestCase):
    def test_compatible_choice_preserves_position_for_analyze(self):
        svc = AdmissionService()
        svc.upsert_tenant("live1", tier="live", guarantee=1, at=0)
        svc.upsert_tenant("live2", tier="live", guarantee=1, at=0)
        svc.upsert_endpoint("flex", skills=["render", "analyze"], cap=1, at=0)
        svc.upsert_endpoint("special", skills=["render"], cap=1, at=0)
        svc.submit_request(id="r", tenant="live1", skill="render", deadline=9, at=1, schedule=False)
        svc.submit_request(id="a", tenant="live2", skill="analyze", deadline=8, at=1, schedule=False)
        out = svc.tick(at=2)
        placement = {d["request_id"]: d["endpoint"] for d in out.dispatches}
        self.assertEqual(placement, {"r": "special", "a": "flex"})

    def test_incompatible_idle_instance_does_not_fulfill_guarantee(self):
        svc = AdmissionService()
        svc.upsert_tenant("live", tier="live", guarantee=1, at=0)
        svc.upsert_tenant("batch", tier="batch", guarantee=0, at=0)
        svc.upsert_endpoint("render-a", skills=["render"], cap=1, at=0)
        svc.upsert_endpoint("index-b", skills=["index"], cap=1, at=0)
        svc.submit_request(id="borrow", tenant="batch", skill="render", deadline=99, at=0)
        # Make its permit explicitly unknown while retaining the physical slot.
        svc.mark_sent(permit="p1", request_id="borrow", endpoint="render-a", epoch=1, at=0)
        svc.report_result("p1", outcome="unknown", endpoint="render-a", epoch=1, at=1)
        svc.submit_request(id="need-render", tenant="live", skill="render", deadline=20, at=2)
        view = svc.inspect_tenant("live")
        reasons = " ".join(s["reason"] for s in view["guarantee_slots"])
        self.assertIn("remote_unknown", reasons)
        self.assertNotIn("index-b", view["guarantee_slots"][0].get("usable_endpoints", []))
        self.assertIn("index-b", view["endpoints"])
        self.assertFalse(view["guarantee_slots"][0]["effective"])

    def test_lowered_cap_keeps_execution_fact_but_blocks_new_admission(self):
        svc = AdmissionService()
        svc.upsert_tenant("live2", tier="live", guarantee=1, at=0)
        svc.upsert_endpoint("flex", skills=["analyze"], cap=1, epoch=1, at=0)
        d = svc.submit_request(id="a", tenant="live2", skill="analyze", deadline=8, at=1)
        svc.mark_sent(permit="pa", request_id="a", endpoint="flex", epoch=1, at=3)
        svc.upsert_endpoint("flex", skills=["render", "analyze"], cap=0, epoch=1, at=4)
        svc.accept("pa", at=5)

        svc.submit_request(id="next", tenant="live2", skill="analyze", deadline=12, at=6)
        view = svc.inspect_tenant("live2", at=7)
        self.assertTrue(view["guarantee_slots"][0]["effective"])
        self.assertEqual(view["guarantee_slots"][0]["request_id"], "a")

        svc.report_result("pa", outcome="done", endpoint="flex", epoch=1, at=8)
        view = svc.inspect_tenant("live2", at=9)
        slot = view["guarantee_slots"][0]
        self.assertFalse(slot["effective"])
        self.assertEqual(slot["reason"], "endpoint_at_zero_capacity")
        self.assertEqual(view["admittable_now"], [])


class FairnessExpiryConfigTests(unittest.TestCase):
    def test_aged_request_blocks_later_same_scope_but_independent_work_continues(self):
        svc = AdmissionService(fairness_delay=3)
        svc.upsert_tenant("old-t", tier="live", guarantee=1, at=0)
        svc.upsert_tenant("flow", tier="live", guarantee=0, at=0)
        # shared is occupied by a nonpreemptible fact until t=6; special is independent.
        svc.upsert_endpoint("shared", skills=["render", "analyze"], cap=1, at=0)
        svc.upsert_endpoint("special", skills=["analyze"], cap=2, at=0)
        d = svc.submit_request(id="blocker", tenant="flow", skill="render",
                               deadline=99, preemptible=False, at=0, schedule=True)
        svc.accept(d.permit_id, at=0)

        svc.submit_request(id="old", tenant="old-t", skill="render", deadline=30, at=1, schedule=False)
        # While not aged, later analyze work can use independent special.
        svc.submit_request(id="later1", tenant="flow", skill="analyze", deadline=30, at=2, schedule=False)
        out = svc.tick(at=2)
        got = {x["request_id"]: x["endpoint"] for x in out.dispatches}
        self.assertEqual(got.get("later1"), "special")
        self.assertNotIn("old", got)

        # More later traffic after old has aged must not consume shared's eventual slot.
        svc.submit_request(id="later2", tenant="flow", skill="analyze", deadline=30, at=5, schedule=False)
        plan = svc.tick(at=5)
        got = {x["request_id"]: x["endpoint"] for x in plan.dispatches}
        self.assertEqual(got.get("later2"), "special")  # independent resource still proceeds
        self.assertNotIn("later2", [x["request_id"] for x in plan.blocked_for_fairness])

        # A later render request is same compatibility scope and cannot jump old.
        svc.submit_request(id="jump", tenant="flow", skill="render", deadline=30, at=6, schedule=False)
        plan = svc.tick(at=6)
        self.assertEqual(plan.dispatches, [])
        self.assertTrue(any(b["request_id"] == "jump" and b["reason"] == "waiting_for_older_request"
                            for b in plan.blocked_for_fairness))

        svc.report_result(d.permit_id, outcome="done", at=7)
        plan = svc.tick(at=7)
        self.assertEqual([x["request_id"] for x in plan.dispatches], ["old"])

    def test_expired_queued_request_not_scheduled_but_admitted_is_tracked(self):
        svc = AdmissionService()
        svc.upsert_tenant("t", tier="live", guarantee=1, at=0)
        svc.upsert_endpoint("e", skills=["x"], cap=1, at=0)
        svc.submit_request(id="held", tenant="t", skill="x", deadline=5, at=0, schedule=True)
        permit = svc.requests["held"]["permit_id"]
        svc.submit_request(id="late", tenant="t", skill="x", deadline=5, at=1, schedule=False)
        out = svc.tick(at=4)
        self.assertEqual(out.dispatches, [])
        self.assertEqual(svc.requests["late"]["status"], "queued")
        out = svc.tick(at=5)
        self.assertEqual(svc.requests["late"]["status"], "expired")
        self.assertEqual(out.dispatches, [])

        # Caller deadline on held execution passes; occupancy and tracking remain.
        svc.tick(at=6)
        self.assertTrue(svc.permits[permit]["caller_gone"])
        ep = svc.inspect_endpoint("e")
        self.assertEqual(ep["active"], 1)
        svc.report_result(permit, outcome="done", at=7)
        self.assertEqual(svc.inspect_endpoint("e")["active"], 0)

    def test_guarantee_change_does_not_revoke_execution_fact(self):
        svc = AdmissionService()
        svc.upsert_tenant("t", tier="live", guarantee=2, at=0)
        svc.upsert_endpoint("e", skills=["x"], cap=2, at=0)
        d1 = svc.submit_request(id="r1", tenant="t", skill="x", deadline=9, at=0, schedule=True)
        d2 = svc.submit_request(id="r2", tenant="t", skill="x", deadline=9, at=0, schedule=True)
        self.assertTrue(d1.admitted and d2.admitted)
        self.assertEqual(svc.inspect_endpoint("e")["active"], 2)
        svc.upsert_tenant("t", tier="live", guarantee=0, at=1)
        view = svc.inspect_tenant("t", at=1)
        self.assertEqual(view["guarantee"], 0)
        self.assertEqual(view["active_online"], 2)
        self.assertEqual(svc.inspect_endpoint("e")["active"], 2)


class RestartAndContractTests(unittest.TestCase):
    def test_restart_and_tenant_wait_limit_preserve_safety(self):
        svc = AdmissionService()
        svc.upsert_tenant("live", tier="online", guarantee=1, max_wait=4, at=0)
        svc.upsert_endpoint("e", skills=["x"], cap=1, at=0)
        d = svc.submit_request(id="old", tenant="live", skill="x", at=0, schedule=True)
        svc.report_result(d.permit_id, "unknown", at=1)
        svc.restart(at=2)
        self.assertEqual(svc.restarts, 1)
        self.assertEqual(svc.inspect_endpoint("e")["active"], 1)

        svc.submit_request(id="waiting", tenant="live", skill="x", at=2, schedule=False)
        self.assertEqual(svc.requests["waiting"]["deadline"], 6)
        with self.assertRaises(AdmissionError):
            svc.mark_sent(permit="duplicate", request_id="old",
                          endpoint="e", epoch=1, at=3)

    def test_local_executor_nonpausable_reports_running_until_done(self):
        from admission_service import LocalExecutionSide
        svc = AdmissionService()
        svc.upsert_tenant("batch", tier="background", guarantee=0, at=0)
        svc.upsert_endpoint("e", skills=["x"], cap=1, at=0)
        d = svc.submit_request(id="slow", tenant="batch", skill="x", preemptible=False, at=0, schedule=True)
        executor = LocalExecutionSide(svc)
        executor.adopt(d.permit_id, at=1)
        p = executor.stop_requested(d.permit_id, at=2)
        self.assertEqual(p["outcome"], "running")
        self.assertEqual(svc.inspect_endpoint("e")["active"], 1)
        # A stop attempt on non-pausable work must not masquerade as a free slot.
        tenant = svc.inspect_tenant("batch")
        self.assertEqual(svc.inspect_endpoint("e")["permits"][0]["state"], "running")
        self.assertNotEqual(tenant["endpoints"]["e"]["available"], 1)
        executor.complete(d.permit_id, at=4)
        self.assertEqual(svc.inspect_endpoint("e")["active"], 0)


class ReplayExamplesTests(unittest.TestCase):
    def test_shipped_examples_replay_and_key_inspections(self):
        root = Path(__file__).parent / "examples"
        results = {}
        for path in sorted(root.glob("*.jsonl")):
            result = replay_jsonl(path)
            results[path.name] = result
            self.assertTrue(result.ok, result.errors)

        first = results["borrow-recall-without-phantom-slot.jsonl"]
        self.assertEqual(first.service.tenants["history"]["class"], "batch")
        live10 = first.views[("tenant", "live")]
        self.assertFalse(live10["guarantee_slots"][0]["effective"])
        self.assertEqual(live10["guarantee_slots"][0]["reason"], "remote_unknown")
        self.assertEqual(first.final_request("live1")["status"], "sent")
        self.assertEqual(first.final_request("live1")["endpoint"], "a")

        second = results["compatible-choice-and-lowered-cap.jsonl"]
        live7 = second.views_at_or_before("tenant", "live2", 7)[-1]
        self.assertTrue(live7["guarantee_slots"][0]["effective"])
        live9 = second.views_at_or_before("tenant", "live2", 9)[-1]
        self.assertEqual(live9["guarantee_slots"][0]["reason"], "endpoint_at_zero_capacity")


if __name__ == "__main__":
    unittest.main()
