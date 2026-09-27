import io
import json
import os
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch
from uuid import UUID

import fuck


class FakeResponse:
    def __init__(self, status):
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class StressTestTests(unittest.TestCase):
    def test_sends_bounded_authenticated_events(self):
        seen = []

        def respond(request, timeout):
            self.assertEqual(timeout, 2.0)
            self.assertEqual(request.get_header("X-webhook-token"), "test-token")
            payload = json.loads(request.data)
            UUID(payload["event_id"])
            seen.append(payload["data"]["sequence"])
            return FakeResponse(202)

        output = io.StringIO()
        with patch.dict(os.environ, {"WEBHOOK_TOKEN": "test-token"}):
            with patch("sys.argv", ["fuck.py", "--requests", "3", "--concurrency", "2", "--timeout", "2"]):
                with patch("fuck.urlopen", side_effect=respond):
                    with redirect_stdout(output):
                        result = fuck.main()

        self.assertEqual(result, 0)
        self.assertEqual(sorted(seen), [0, 1, 2])
        self.assertIn("sent=3 accepted=3 failed=0", output.getvalue())

    def test_returns_failure_for_rejected_requests(self):
        output = io.StringIO()
        with patch.dict(os.environ, {"WEBHOOK_TOKEN": "test-token"}):
            with patch("sys.argv", ["fuck.py", "--requests", "1"]):
                with patch("fuck.urlopen", return_value=FakeResponse(401)):
                    with redirect_stdout(output):
                        result = fuck.main()

        self.assertEqual(result, 1)
        self.assertIn("accepted=0 failed=1", output.getvalue())
