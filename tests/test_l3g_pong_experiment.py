from dataclasses import replace
from decimal import Decimal
import unittest

from src.l3g_paper.daily_bias import (
    DailyBiasDirection as D, DailyBiasEngine, BiasUpdateReason, OptionsDayClass,
    OptionsOverlayOutcome, PONG_TRIAL_OPTIONS_AUTHORITY,
)
from src.l3g_paper.pong_experiment import (
    CompletedPongHour, hourly_structure_components,
    unavailable_nonvoting_options, experimental_definition,
)
from src.l3g_paper.pong_microstructure import MicrostructureEvidenceMappingPolicy
from tests.test_l3g_daily_bias import frame, options, START, END, DECISION


def hour(start, end, o, h, l, c, identity):
    return CompletedPongHour(start, end, end, *(Decimal(str(x)) for x in (o,h,l,c)),
                             'AUTHENTIC_ADAPTER_FIXTURE', 'domain-1', (identity,), ('a'*64,), True)


class PongExperimentTests(unittest.TestCase):
    def setUp(self):
        self.prior = hour('2026-09-11T08:00:00Z','2026-09-11T09:00:00Z',100,110,90,101,'prior')
        self.current = hour('2026-09-11T09:00:00Z','2026-09-11T10:00:00Z',101,115,95,111,'current')

    def test_hourly_trend_is_mirrored_and_strict(self):
        trend, crt = hourly_structure_components(self.prior,self.current,available_at=self.current.end)
        self.assertEqual(trend.direction,D.LONG)
        self.assertEqual(crt.direction,D.NEUTRAL)
        mirrored=replace(self.current,open=Decimal(99),high=Decimal(105),low=Decimal(85),close=Decimal(89))
        trend, crt=hourly_structure_components(self.prior,mirrored,available_at=mirrored.end)
        self.assertEqual(trend.direction,D.SHORT)
        self.assertEqual(crt.direction,D.NEUTRAL)
        trend,_=hourly_structure_components(self.prior,replace(self.current,low=self.prior.low),available_at=self.current.end)
        self.assertEqual(trend.direction,D.NEUTRAL)

    def test_crt_sweep_reentry_mirrors_and_both_sides_are_neutral(self):
        for high,low,close,expected in [(105,85,100,D.LONG),(115,95,100,D.SHORT),(115,85,100,D.NEUTRAL),(105,95,100,D.NEUTRAL),(105,85,90,D.NEUTRAL)]:
            with self.subTest(high=high,low=low,close=close):
                value=replace(self.current,high=Decimal(high),low=Decimal(low),close=Decimal(close))
                _,crt=hourly_structure_components(self.prior,value,available_at=value.end)
                self.assertEqual(crt.direction,expected)

    def test_partial_future_gap_mixed_domain_and_late_publication_fail(self):
        for bad in [replace(self.current,complete=False),replace(self.current,continuity_domain='other'),replace(self.current,available_at='2026-09-11T10:01:00Z')]:
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                hourly_structure_components(self.prior,bad,available_at=self.current.end)
        with self.assertRaises(ValueError):
            hourly_structure_components(self.prior,self.current,available_at='2026-09-11T10:05:00.000001Z')
        with self.assertRaises(ValueError):
            hourly_structure_components(replace(self.prior,start='2026-09-11T07:00:00Z',end='2026-09-11T08:00:00Z'),self.current,available_at=self.current.end)

    def test_source_changes_propagate_to_component_provenance(self):
        first=hourly_structure_components(self.prior,self.current,available_at=self.current.end)[0]
        second=hourly_structure_components(self.prior,replace(self.current,source_hashes=('b'*64,)),available_at=self.current.end)[0]
        self.assertNotEqual(first.evidence_hash,second.evidence_hash)

    def test_nonvoting_absence_preserves_core_without_credit(self):
        absent=unavailable_nonvoting_options(start=START,end=END,available_at=DECISION)
        self.assertEqual(absent.day_class,OptionsDayClass.UNKNOWN)
        self.assertEqual(absent.outcome,OptionsOverlayOutcome.UNAVAILABLE)
        self.assertFalse(absent.required)
        for directions in [(D.LONG,D.LONG,D.NEUTRAL),(D.SHORT,D.SHORT,D.SHORT),(D.LONG,D.SHORT,D.NEUTRAL)]:
            ordinary=DailyBiasEngine().update(frame(directions), BiasUpdateReason.COMPLETED_HOURLY_BOUNDARY).result
            missing=DailyBiasEngine().update(frame(directions,overlay=absent), BiasUpdateReason.COMPLETED_HOURLY_BOUNDARY).result
            self.assertEqual((ordinary.direction,ordinary.confidence),(missing.direction,missing.confidence))
            self.assertIn('OPTIONS_UNAVAILABLE_NON_VOTING_EXPERIMENT_ONLY',missing.reason_codes)
            self.assertNotEqual(ordinary.result_hash,missing.result_hash)

    def test_nonvoting_requires_exact_scope_and_cannot_confirm(self):
        absent=unavailable_nonvoting_options(start=START,end=END,available_at=DECISION)
        for changes in [dict(experimental_nonvoting_authority=None),dict(experimental_nonvoting_authority='b'*64),dict(outcome=OptionsOverlayOutcome.CONFIRM,direction=D.LONG),dict(day_class=OptionsDayClass.ORDINARY),dict(required=True)]:
            with self.subTest(changes=changes),self.assertRaises(ValueError): replace(absent,**changes)
        self.assertEqual(absent.payload()['experimental_nonvoting_authority'],PONG_TRIAL_OPTIONS_AUTHORITY)
        self.assertNotIn('experimental_nonvoting_authority',options().payload())

    def test_legacy_unknown_options_still_denies(self):
        value=DailyBiasEngine().update(frame((D.LONG,D.LONG,D.LONG),overlay=options(OptionsOverlayOutcome.UNAVAILABLE)), BiasUpdateReason.COMPLETED_HOURLY_BOUNDARY).result
        self.assertEqual(value.direction,D.UNAVAILABLE)

    def test_research_mapping_and_thresholds_are_reused(self):
        policy=MicrostructureEvidenceMappingPolicy().discernment_policy()
        self.assertEqual((policy.long_on_threshold,policy.short_on_threshold),(0.65,0.35))
        self.assertEqual(experimental_definition()['discernment_policy_hash'],policy.policy_hash)
        self.assertEqual(experimental_definition()['maximum_execution_quantity'],1)


if __name__ == '__main__': unittest.main()
