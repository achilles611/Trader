from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from src.l3h_live.gateway import GatewayProtocolError, ReplayGuard, sign_frame, verify_frame


class L3HTransportTests(unittest.TestCase):
    key = b"k" * 32

    def frame(self) -> dict[str, object]:
        return sign_frame({
            "message_type": "COMMAND", "request_id": "request-transport-001", "nonce": "nonce-transport-001",
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "payload": {"command_id": "l3h-cmd-unit", "client_order_id": "BZ-L3H-UNIT", "quantity": 1},
        }, self.key)

    def test_valid_signed_frame_is_accepted_once(self) -> None:
        guard = ReplayGuard()
        verified = verify_frame(self.frame(), self.key, replay_guard=guard)
        self.assertEqual((verified.message_type, verified.payload["quantity"]), ("COMMAND", 1))
        with self.assertRaisesRegex(GatewayProtocolError, "DENY_REPLAY"):
            verify_frame(self.frame(), self.key, replay_guard=guard)

    def test_altered_payload_wrong_key_and_stale_timestamp_are_denied(self) -> None:
        altered = self.frame(); altered["payload"] = {"command_id": "l3h-cmd-unit", "client_order_id": "BZ-L3H-UNIT", "quantity": 2}
        with self.assertRaisesRegex(GatewayProtocolError, "PAYLOAD_HASH"):
            verify_frame(altered, self.key)
        with self.assertRaisesRegex(GatewayProtocolError, "BAD_SIGNATURE"):
            verify_frame(self.frame(), b"x" * 32)
        stale = sign_frame({
            "message_type": "COMMAND", "request_id": "request-stale-001", "nonce": "nonce-stale-001",
            "timestamp": (datetime.now(timezone.utc) - timedelta(seconds=11)).isoformat().replace("+00:00", "Z"),
            "payload": {"command_id": "l3h-cmd-unit", "client_order_id": "BZ-L3H-UNIT", "quantity": 1},
        }, self.key)
        with self.assertRaisesRegex(GatewayProtocolError, "STALE_TIMESTAMP"):
            verify_frame(stale, self.key)

    def test_missing_or_extra_protocol_fields_are_denied(self) -> None:
        frame = self.frame(); del frame["signature"]
        with self.assertRaisesRegex(GatewayProtocolError, "PROTOCOL_FIELDS"):
            verify_frame(frame, self.key)


if __name__ == "__main__":
    unittest.main()
