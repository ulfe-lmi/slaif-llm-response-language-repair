"""EXPERIMENTAL / CONCEPT VERIFICATION / NOT PRODUCTION CODE. Review policy.

Order 040-a acceptance coverage for the focused typed policy/loader:
shipped default file, canonical discovery precedence (explicit path >
CONCEPT_CONFIG > shipped adjacent file, never historical search), strict
malformed-config failure BEFORE any listener/model/corpus work at the
actual main entrypoint (fixed content-free reason, no value echo),
length-scaled allowance math on Unicode code points, the wire
reasoning.effort default and nondefaults, configured batch/prompt/output
bounds reaching the pure batch builders, and the legacy
CONCEPT_REVIEWER_TIMEOUT precedence validated equally before bind.

Real HTTP, the owned transport child and live model calls stay outside
this file (test_review_batch / test_review_transport / the secondary
integration file own those boundaries).
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import unittest
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import cast
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import batch_review  # noqa: E402
import proxy  # noqa: E402
import review_policy as rp  # noqa: E402
from batch_review import (  # noqa: E402
    BatchProposal,
    BatchResult,
    BatchUsage,
    build_prompt,
    build_request_body,
    prepare_targets,
)
from qwen_client import ResponsesReviewer  # noqa: E402

TEXT = "To je presenetliv zapisano, to je presenetliv zapisano."


def _write(tmp: Path, name: str, text: str) -> Path:
    path = tmp / name
    path.write_text(text, encoding="utf-8")
    return path


class ShippedFileTests(unittest.TestCase):
    def test_shipped_file_has_exact_nine_fields_and_defaults(self) -> None:
        self.assertTrue(rp.SHIPPED_CONFIG_PATH.is_file())
        policy = rp.load_review_policy(rp.SHIPPED_CONFIG_PATH)
        self.assertEqual(
            policy,
            rp.ReviewPolicy(
                characters_per_call=1024,
                targets_per_call=4,
                context_codepoints=240,
                max_prompt_codepoints=8192,
                max_output_tokens=1536,
                max_request_bytes=32768,
                max_response_bytes=32768,
                timeout_seconds=15.0,
                reasoning_effort="low",
            ),
        )

    def test_discovery_precedence_explicit_env_shipped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            explicit = _write(
                tmp_path,
                "explicit.toml",
                "[review]\ncharacters_per_call = 512\n",
            )
            env_file = _write(
                tmp_path,
                "env.toml",
                "[review]\ncharacters_per_call = 2048\n",
            )
            with mock.patch.dict(os.environ, {rp.REVIEW_CONFIG_ENV: str(env_file)}):
                # Explicit supplied path wins over the environment.
                self.assertEqual(
                    rp.load_review_policy(explicit).characters_per_call, 512
                )
                # Environment wins over the shipped adjacent file.
                self.assertEqual(
                    rp.load_review_policy().characters_per_call, 2048
                )
            # Without explicit path or environment: ONLY the shipped
            # adjacent file, never a directory search.
            self.assertEqual(
                rp.load_review_policy().characters_per_call, 1024
            )

    def test_shipped_file_selected_when_env_unset(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(rp.REVIEW_CONFIG_ENV, None)
            self.assertEqual(
                rp.load_review_policy().characters_per_call,
                1024,
            )


class AllowanceMathTests(unittest.TestCase):
    def test_default_allowance_boundaries(self) -> None:
        policy = rp.DEFAULT_POLICY
        for n, expected in (
            (0, 0),
            (1, 1),
            (1023, 1),
            (1024, 1),
            (1025, 2),
            (2048, 2),
            (2049, 3),
            (32768, 32),
        ):
            self.assertEqual(policy.review_call_allowance(n), expected, n)

    def test_unicode_codepoints_not_bytes(self) -> None:
        # 512 U+0161 (s-caron) code points = 1024 UTF-8 bytes; the
        # allowance is on code points, so B = 1.
        text = "\u0161" * 512
        self.assertEqual(len(text.encode("utf-8")), 1024)
        self.assertEqual(rp.DEFAULT_POLICY.review_call_allowance(len(text)), 1)

    def test_selection_maximum_scales_with_targets_per_call(self) -> None:
        policy = rp.DEFAULT_POLICY
        self.assertEqual(policy.selection_maximum(0), 0)
        self.assertEqual(policy.selection_maximum(1024), 4)
        self.assertEqual(policy.selection_maximum(1025), 8)
        scaled = replace(policy, characters_per_call=100, targets_per_call=3)
        self.assertEqual(scaled.selection_maximum(250), 9)

    def test_custom_characters_per_call_boundaries(self) -> None:
        policy = replace(rp.DEFAULT_POLICY, characters_per_call=100)
        for n, expected in (
            (0, 0),
            (1, 1),
            (99, 1),
            (100, 1),
            (101, 2),
            (200, 2),
            (201, 3),
        ):
            self.assertEqual(policy.review_call_allowance(n), expected, n)


class MalformedConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _expect(self, body: str, label: str, filename: str = "bad.toml") -> None:
        path = _write(self.tmp, filename, body)
        with self.assertRaises(rp.ReviewPolicyError) as ctx:
            rp.load_review_policy(path)
        self.assertEqual(ctx.exception.label, label)
        # No offending value echo: a unique sentinel value must not
        # appear in the diagnostic.
        self.assertNotIn("sentinel-value", str(ctx.exception))

    def test_missing_file(self) -> None:
        with self.assertRaises(rp.ReviewPolicyError) as ctx:
            rp.load_review_policy(self.tmp / "absent.toml")
        self.assertEqual(ctx.exception.label, "file missing or unreadable")

    def test_unreadable_path_is_directory(self) -> None:
        with self.assertRaises(rp.ReviewPolicyError) as ctx:
            rp.load_review_policy(self.tmp)
        self.assertEqual(ctx.exception.label, "file missing or unreadable")

    def test_malformed_toml(self) -> None:
        self._expect(
            "[review\ncharacters_per_call = 1024\n", "malformed toml"
        )

    def test_invalid_utf8_is_malformed_not_crash(self) -> None:
        path = self.tmp / "binary.toml"
        path.write_bytes(b"[review]\ncharacters_per_call = 1024\n\xff\xfe\n")
        with self.assertRaises(rp.ReviewPolicyError) as ctx:
            rp.load_review_policy(path)
        self.assertEqual(ctx.exception.label, "malformed toml")

    def test_missing_review_section(self) -> None:
        # A clean missing-[review] example is the empty document.
        self._expect("", "missing review section")

    def test_only_other_section_is_unknown_section(self) -> None:
        # A document containing only [other] is an unknown section,
        # not a missing review section.
        self._expect("[other]\nkey = 1", "unknown section")

    def test_review_not_a_table(self) -> None:
        self._expect("review = 5", "wrong type")

    def test_unknown_top_level_section(self) -> None:
        self._expect("[review]\n\n[other]\nkey = 1", "unknown section")

    def test_unknown_top_level_key(self) -> None:
        self._expect("topkey = 1\n\n[review]\n", "unknown section")

    def test_unknown_review_key(self) -> None:
        self._expect(
            "[review]\ncharacters_per_call = 1024\nunknown_key = 1\n",
            "unknown key",
        )

    def test_bool_as_int_rejected(self) -> None:
        self._expect(
            "[review]\ntargets_per_call = true\n", "wrong type"
        )

    def test_float_as_int_rejected(self) -> None:
        self._expect(
            "[review]\ntargets_per_call = 4.0\n", "wrong type"
        )

    def test_zero_int_rejected(self) -> None:
        self._expect(
            "[review]\ncharacters_per_call = 0\n", "invalid bound"
        )

    def test_negative_int_rejected(self) -> None:
        self._expect(
            "[review]\nmax_request_bytes = -1\n", "invalid bound"
        )

    def test_timeout_zero_rejected(self) -> None:
        self._expect("[review]\ntimeout_seconds = 0\n", "invalid bound")

    def test_timeout_negative_rejected(self) -> None:
        self._expect("[review]\ntimeout_seconds = -2.5\n", "invalid bound")

    def test_timeout_nan_rejected(self) -> None:
        self._expect("[review]\ntimeout_seconds = nan\n", "invalid bound")

    def test_timeout_inf_rejected(self) -> None:
        self._expect("[review]\ntimeout_seconds = inf\n", "invalid bound")

    def test_timeout_1e999_rejected(self) -> None:
        self._expect("[review]\ntimeout_seconds = 1e999\n", "invalid bound")

    def test_timeout_huge_int_rejected_not_crash(self) -> None:
        self._expect(
            f"[review]\ntimeout_seconds = {10**1000}\n", "invalid bound"
        )

    def test_timeout_bool_rejected(self) -> None:
        self._expect("[review]\ntimeout_seconds = true\n", "wrong type")

    def test_unknown_reasoning_effort(self) -> None:
        self._expect(
            '[review]\nreasoning_effort = "turbo"\n', "unknown reasoning effort"
        )

    def test_reasoning_effort_non_string(self) -> None:
        self._expect('[review]\nreasoning_effort = 3\n', "unknown reasoning effort")

    def test_all_standard_effort_names_accepted(self) -> None:
        for name in rp.REASONING_EFFORTS:
            path = _write(
                self.tmp,
                f"effort-{name}.toml",
                f'[review]\nreasoning_effort = "{name}"\n',
            )
            self.assertEqual(
                rp.load_review_policy(path).reasoning_effort, name
            )

    def test_absent_keys_take_defaults(self) -> None:
        path = _write(self.tmp, "partial.toml", "[review]\n")
        self.assertEqual(rp.load_review_policy(path), rp.DEFAULT_POLICY)

    def test_partial_override_keeps_other_defaults(self) -> None:
        path = _write(
            self.tmp, "partial2.toml", "[review]\ntargets_per_call = 8\n"
        )
        policy = rp.load_review_policy(path)
        self.assertEqual(policy, replace(rp.DEFAULT_POLICY, targets_per_call=8))

    def test_byte_cap_above_frame_field_rejected(self) -> None:
        self._expect(
            "[review]\nmax_response_bytes = 4294967296\n", "invalid bound"
        )


class TimeoutOverrideValidationTests(unittest.TestCase):
    def test_valid_int_float_accepted(self) -> None:
        self.assertEqual(rp.validate_reviewer_timeout(7), 7.0)
        self.assertEqual(rp.validate_reviewer_timeout(7.5), 7.5)

    def test_invalid_values_rejected(self) -> None:
        for bad in (True, False, 0, -1, 0.0, float("inf"), float("nan"), 10**400, "x"):
            with self.assertRaises(rp.ReviewPolicyError, msg=repr(bad)):
                rp.validate_reviewer_timeout(bad)


class WireTests(unittest.TestCase):
    def test_default_wire_field_is_low(self) -> None:
        body = json.loads(build_request_body("m", "prompt"))
        self.assertEqual(body["reasoning"], {"effort": "low"})
        self.assertEqual(body["max_output_tokens"], 1536)
        self.assertIs(body["stream"], False)
        self.assertIs(body["store"], False)

    def test_nondefault_effort_and_tokens_reach_wire(self) -> None:
        policy = replace(
            rp.DEFAULT_POLICY,
            reasoning_effort="high",
            max_output_tokens=2048,
        )
        body = json.loads(build_request_body("m", "prompt", policy))
        self.assertEqual(body["reasoning"], {"effort": "high"})
        self.assertEqual(body["max_output_tokens"], 2048)

    def test_prompt_limit_from_policy(self) -> None:
        policy = replace(rp.DEFAULT_POLICY, max_prompt_codepoints=10)
        targets = prepare_targets(TEXT, ((6, 17, "presenetliv"),), ())
        self.assertIsNotNone(targets)
        self.assertIsNone(build_prompt(cast(tuple, targets), policy))
        self.assertIsNotNone(build_prompt(cast(tuple, targets), rp.DEFAULT_POLICY))

    def test_context_window_from_policy(self) -> None:
        # Long prefix: the window is exactly the configured code points.
        long_text = ("x" * 300) + " presenetliv"
        long_occurrences = ((301, 312, "presenetliv"),)
        default = prepare_targets(long_text, long_occurrences, ())
        assert default is not None
        self.assertEqual(len(default[0].before_target), 240)
        self.assertTrue(default[0].truncated_before)
        small = replace(rp.DEFAULT_POLICY, context_codepoints=3)
        targets = prepare_targets(long_text, long_occurrences, (), small)
        assert targets is not None
        self.assertEqual(len(targets[0].before_target), 3)
        self.assertTrue(targets[0].truncated_before)
        # Short prefix: bounded by the text start, never padded.
        short = prepare_targets(TEXT, ((6, 17, "presenetliv"),), ())
        assert short is not None
        self.assertEqual(len(short[0].before_target), 6)
        self.assertEqual(short[0].before_target, "To je ")
        self.assertFalse(short[0].truncated_before)

    def test_targets_per_call_from_policy(self) -> None:
        occurrences = _occurrences(TEXT)
        self.assertEqual(len(occurrences), 2)
        scaled = replace(rp.DEFAULT_POLICY, targets_per_call=1)
        self.assertIsNone(prepare_targets(TEXT, occurrences, (), scaled))
        self.assertIsNotNone(prepare_targets(TEXT, occurrences, ()))

    def test_id_offset_keeps_ids_unique_across_batches(self) -> None:
        text = "a a a a a a"
        occurrences = _occurrences(text, "a")
        self.assertEqual(len(occurrences), 6)
        first = prepare_targets(text, occurrences[:4], (), id_offset=0)
        second = prepare_targets(text, occurrences[4:], (), id_offset=4)
        assert first is not None and second is not None
        ids = [t.id for t in first] + [t.id for t in second]
        self.assertEqual(ids, ["t0", "t1", "t2", "t3", "t4", "t5"])


def _occurrences(text: str, word: str = "presenetliv") -> tuple[tuple[int, int, str], ...]:
    result: list[tuple[int, int, str]] = []
    start = text.find(word)
    while start != -1:
        result.append((start, start + len(word), word))
        start = text.find(word, start + 1)
    return tuple(result)


class RealEntrypointWiringTests(unittest.TestCase):
    """Actual proxy.main() constructor/main wiring boundary (order
    040-a, R3).  A malformed or invalid review configuration refuses
    startup at the named pre-bind point with a fixed content-free
    reason and no listener/model/corpus work; the real normal
    construction carries the SAME immutable policy instance to the real
    reviewer and the real engine, with the legacy environment timeout
    override validated equally before bind.

    Boundary label: constructor/main wiring with a real local bind on an
    owned ephemeral port and an intentionally stopped serve loop - not
    full network/model success; the secondary integration file owns
    full real-HTTP behavior."""

    def setUp(self) -> None:
        configured = os.environ.get("CONCEPT_PANDOC", "").strip()
        if not configured:
            raise unittest.SkipTest("qualified CONCEPT_PANDOC not configured")
        self.pandoc = configured
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        # A real index file keeps the environment a fully valid normal
        # startup except for the one invalid setting under test.
        self.index = self.tmp / "idx.sqlite"
        connection = sqlite3.connect(self.index)
        connection.executescript(
            """
            CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE unigram (word TEXT PRIMARY KEY, count INTEGER NOT NULL);
            CREATE TABLE bigram (phrase TEXT PRIMARY KEY, count INTEGER NOT NULL);
            CREATE TABLE trigram (phrase TEXT PRIMARY KEY, count INTEGER NOT NULL);
            CREATE TABLE middle (left_word TEXT, right_word TEXT, middle_word TEXT, count INTEGER,
                PRIMARY KEY(left_word,right_word,middle_word));
            """
        )
        connection.commit()
        connection.close()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _env(self, **extra: str) -> dict[str, str]:
        env = {
            "CONCEPT_INDEX": str(self.index),
            "CONCEPT_PANDOC": self.pandoc,
        }
        env.update(extra)
        return env

    @staticmethod
    def _prebind_guard(records: list[object], name: str) -> Callable[..., object]:
        # Named pre-bind boundary guard: any construction of the model
        # reviewer, the engine (corpus) or the server (listener/bind)
        # before the review policy is validated is a wiring failure.
        def guard(*args: object, **kwargs: object) -> None:
            records.append((args, kwargs))
            raise AssertionError(
                f"{name} constructed before the pre-bind review-policy boundary"
            )

        return guard

    def _expect_prebind_refusal(
        self, env: dict[str, str], fragment: str, must_not_contain: str
    ) -> None:
        reviewer_calls: list[object] = []
        engine_calls: list[object] = []
        server_calls: list[object] = []
        with (
            mock.patch.dict(os.environ, env, clear=True),
            mock.patch.object(
                proxy,
                "ResponsesReviewer",
                self._prebind_guard(reviewer_calls, "ResponsesReviewer"),
            ),
            mock.patch.object(
                proxy,
                "RepairEngine",
                self._prebind_guard(engine_calls, "RepairEngine"),
            ),
            mock.patch.object(
                proxy,
                "ConceptProxy",
                self._prebind_guard(server_calls, "ConceptProxy"),
            ),
            self.assertRaises(SystemExit) as ctx,
        ):
            proxy.main()
        message = str(ctx.exception)
        self.assertIn(fragment, message)
        # No value echo of the invalid operator input.
        self.assertNotIn(must_not_contain, message)
        # No listener, model or corpus work at the named pre-bind point.
        self.assertEqual(reviewer_calls, [])
        self.assertEqual(engine_calls, [])
        self.assertEqual(server_calls, [])

    def test_malformed_config_refuses_startup_before_bind(self) -> None:
        bad = _write(
            self.tmp, "bad.toml", '[review]\nunknown_key = "sentinel-value"\n'
        )
        env = self._env(**{rp.REVIEW_CONFIG_ENV: str(bad)})
        self._expect_prebind_refusal(
            env, "review config: unknown key", "sentinel-value"
        )

    def test_invalid_legacy_timeout_refuses_startup_before_bind(self) -> None:
        env = self._env(CONCEPT_REVIEWER_TIMEOUT="sentinel-duration")
        self._expect_prebind_refusal(
            env, "CONCEPT_REVIEWER_TIMEOUT", "sentinel-duration"
        )

    def test_huge_legacy_timeout_refuses_startup_not_crash(self) -> None:
        huge = "1" + "0" * 400
        env = self._env(CONCEPT_REVIEWER_TIMEOUT=huge)
        self._expect_prebind_refusal(
            env, "CONCEPT_REVIEWER_TIMEOUT", huge
        )

    def test_nondefault_policy_and_timeout_reach_engine_and_reviewer(
        self,
    ) -> None:
        config = _write(
            self.tmp,
            "nondefault.toml",
            "[review]\ncharacters_per_call = 512\ntimeout_seconds = 7.5\n",
        )
        seen: dict[str, object] = {}
        created: list[proxy.ConceptProxy] = []
        reviewers: list[ResponsesReviewer] = []

        class Recorder(ResponsesReviewer):
            def __init__(
                self,
                base_url: str,
                model: str,
                timeout: float | None = None,
                api_key: str | None = None,
                profile: str | Path | None = None,
                policy: rp.ReviewPolicy = rp.DEFAULT_POLICY,
            ) -> None:
                super().__init__(
                    base_url,
                    model,
                    timeout=timeout,
                    api_key=api_key,
                    profile=profile,
                    policy=policy,
                )
                reviewers.append(self)

        class Port0Proxy(proxy.ConceptProxy):
            def __init__(
                self,
                address: tuple[str, int],
                upstream: str,
                engine: proxy.RepairEngine,
                timeout: float = 30.0,
                trace_dir: Path | None = None,
                usage_observer: (
                    Callable[[dict[str, object]], None] | None
                ) = None,
                *,
                header_timeout: float = proxy.MAIN_HEADER_TIMEOUT_SECONDS,
                client_timeout: float = proxy.CLIENT_REQUEST_TIMEOUT_SECONDS,
                delivery_timeout: float = proxy.CLIENT_DELIVERY_TIMEOUT_SECONDS,
                setup_timeout: float = proxy.MAIN_SETUP_TIMEOUT_SECONDS,
                passthrough_mode: bool = False,
            ) -> None:
                # The real constructor binds an owned ephemeral port
                # (never a fixed service port); the stopped serve loop
                # lets main() run its real owning close.
                super().__init__(
                    ("127.0.0.1", 0),
                    upstream,
                    engine,
                    timeout=timeout,
                    trace_dir=trace_dir,
                    usage_observer=usage_observer,
                    header_timeout=header_timeout,
                    client_timeout=client_timeout,
                    delivery_timeout=delivery_timeout,
                    setup_timeout=setup_timeout,
                    passthrough_mode=passthrough_mode,
                )
                seen["engine"] = engine
                created.append(self)

            def serve_forever(self, poll_interval: float = 0.5) -> None:
                return

        env = self._env(
            **{
                rp.REVIEW_CONFIG_ENV: str(config),
                "CONCEPT_REVIEWER_TIMEOUT": "9.25",
            }
        )
        with (
            mock.patch.dict(os.environ, env, clear=True),
            mock.patch.object(proxy, "ResponsesReviewer", Recorder),
            mock.patch.object(proxy, "ConceptProxy", Port0Proxy),
        ):
            self.assertEqual(proxy.main(), 0)

        self.assertEqual(len(created), 1)
        self.assertEqual(len(reviewers), 1)
        server = created[0]
        # Actual local bind on the owned ephemeral port, never the fixed
        # default service port.
        self.assertEqual(server.server_address[0], "127.0.0.1")
        self.assertGreater(server.server_address[1], 1024)
        self.assertNotEqual(server.server_address[1], 18024)
        # The real constructed instances, observed transparently before
        # the stopped serve loop: the server holds exactly the recorded
        # engine, and that engine's reviewer is exactly the recorded
        # reviewer instance.
        self.assertIs(server.engine, seen["engine"])
        reviewer = cast(ResponsesReviewer, server.engine.reviewer)
        self.assertIs(reviewer, reviewers[0])
        # The environment override wins over the file (9.25 > 7.5) and
        # the file value (512) reached the real runtime; a dropped file
        # or environment setting changes one of these assertions.
        self.assertEqual(reviewer.timeout, 9.25)
        self.assertEqual(reviewer.policy.characters_per_call, 512)
        self.assertEqual(reviewer.policy.timeout_seconds, 9.25)
        self.assertEqual(reviewer.policy.reasoning_effort, "low")
        # The SAME immutable policy instance reaches engine and
        # reviewer (order 040-a interface checkpoint).
        self.assertIs(server.engine.policy, reviewer.policy)
        self.assertIsNot(server.engine.policy, rp.DEFAULT_POLICY)


if __name__ == "__main__":
    unittest.main()
