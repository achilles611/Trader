from __future__ import annotations

import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from src.eth_bot.config import BotConfig
from src.eth_bot.profiles import build_swarm_instance_configs
from src.profile_loader import load_bot_definitions, load_runtime_settings


class GenerationProposalTests(unittest.TestCase):
    def _write_generation_9_proposals(self, root: Path) -> None:
        proposal_dir = root / "reports" / "generation_009"
        proposal_dir.mkdir(parents=True, exist_ok=True)
        proposal_dir.joinpath("next_generation_proposals.json").write_text(
            json.dumps(
                {
                    "to_generation": 10,
                    "proposals": [
                        {
                            "instance_id": "tr3",
                            "mode": "mutated_child",
                            "profile": {
                                "entry_threshold_long": 0.7602470484816383,
                                "entry_threshold_short": 0.7822159380540696,
                                "weight_trend": 0.9595169979743047,
                                "weight_pullback": 1.5866238449790644,
                                "weight_momentum": 0.7915838150140582,
                                "weight_cross": 0.4731470794335148,
                                "weight_rsi": 0.9898801623275901,
                                "weight_near_extreme_penalty": 1.0366143821223428,
                                "weight_network": 0.17148443293480603,
                                "rule_weight": 0.8638154457318917,
                                "exploration_bonus": 0.0,
                                "block_entries_in_chop": True,
                                "allow_near_recent_high_long": False,
                                "allow_near_recent_low_short": False,
                                "allow_countertrend": False,
                                "max_hold_seconds": 3600,
                                "cooldown_after_loss_seconds": 180,
                                "cooldown_after_win_seconds": 45,
                                "flip_cooldown_seconds": 180,
                                "min_confirmation_signals": 3,
                                "aggressive_entries": False,
                                "long_bias": 0.0,
                                "short_bias": 0.0,
                            },
                        },
                        {
                            "instance_id": "tr7",
                            "mode": "exploratory_outlier",
                            "profile": {
                                "entry_threshold_long": 0.7249018354648386,
                                "entry_threshold_short": 0.7940533703952541,
                                "weight_trend": 0.8343433282840893,
                                "weight_pullback": 0.8846059583655695,
                                "weight_momentum": 1.2016190927833135,
                                "weight_cross": 1.299465759133485,
                                "weight_rsi": 1.1261979460637754,
                                "weight_near_extreme_penalty": 1.1340972684303248,
                                "weight_network": 0.1657997424589738,
                                "rule_weight": 0.6693108857758993,
                                "exploration_bonus": 0.0,
                                "block_entries_in_chop": True,
                                "allow_near_recent_high_long": False,
                                "allow_near_recent_low_short": False,
                                "allow_countertrend": False,
                                "max_hold_seconds": 3600,
                                "cooldown_after_loss_seconds": 180,
                                "cooldown_after_win_seconds": 45,
                                "flip_cooldown_seconds": 180,
                                "min_confirmation_signals": 3,
                                "aggressive_entries": False,
                                "long_bias": 0.0,
                                "short_bias": 0.09054907662888283,
                            },
                        },
                    ],
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    def test_build_swarm_instance_configs_applies_previous_generation_proposals(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_generation_9_proposals(root)
            config = BotConfig.from_env()

            instances = build_swarm_instance_configs(config, generation=10, root_dir=root)
            by_id = {instance.instance_id: instance for instance in instances}

            self.assertAlmostEqual(by_id["tr3"].strategy_profile.entry_threshold_long, 0.7602470484816383)
            self.assertAlmostEqual(by_id["tr7"].strategy_profile.short_bias, 0.09054907662888283)
            self.assertAlmostEqual(by_id["tr1"].strategy_profile.entry_threshold_long, 0.72)

    def test_load_bot_definitions_applies_previous_generation_proposals(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            shutil.copytree(repo_root / "config", root / "config")
            self._write_generation_9_proposals(root)

            original_generation = os.environ.get("SWARM_GENERATION")
            try:
                os.environ["SWARM_GENERATION"] = "10"
                settings = load_runtime_settings(root)
                definitions = load_bot_definitions(settings)
            finally:
                if original_generation is None:
                    os.environ.pop("SWARM_GENERATION", None)
                else:
                    os.environ["SWARM_GENERATION"] = original_generation

            by_id = {definition.bot_id: definition for definition in definitions}
            self.assertAlmostEqual(by_id["tr3"].strategy_profile.entry_threshold_long, 0.7602470484816383)
            self.assertAlmostEqual(by_id["tr7"].strategy_profile.short_bias, 0.09054907662888283)
            self.assertAlmostEqual(by_id["zerk1"].strategy_profile.entry_threshold_long, 0.42)


if __name__ == "__main__":
    unittest.main()
