"""Contract checks for benchmark accounting and the loopback Range server."""

import os
from pathlib import Path
import sys
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from run import RangeServer, isolated


class HarnessTests(unittest.TestCase):
    def test_http_head_ranges_and_accounting(self):
        root = Path(__file__).resolve().parents[1] / "testdata" / "bytes"
        name = "requested.bin"
        expected = (root / name).read_bytes()
        server = RangeServer(root, 0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        endpoint = f"http://127.0.0.1:{server.server_port}"
        try:
            with urlopen(Request(f"{endpoint}/{name}", method="HEAD")) as response:
                self.assertEqual(int(response.headers["Content-Length"]), len(expected))
                self.assertEqual(response.read(), b"")
            with urlopen(
                Request(f"{endpoint}/{name}", headers={"Range": "bytes=1-3"})
            ) as response:
                self.assertEqual(response.status, 206)
                self.assertEqual(
                    response.headers["Content-Range"], f"bytes 1-3/{len(expected)}"
                )
                self.assertEqual(response.read(), expected[1:4])
            with urlopen(f"{endpoint}/{name}") as response:
                self.assertEqual(response.read(), expected)
            for path, status in [("../outside", 404), (name, 416)]:
                request = Request(
                    f"{endpoint}/{path}", headers={"Range": "bytes=999999-"}
                )
                with self.assertRaises(HTTPError) as error:
                    urlopen(request)
                self.assertEqual(error.exception.code, status)
            # Complete responses before checking the counters.
            server.shutdown()
            counts = server.snapshot()
            self.assertEqual(counts["HEAD"], 1)
            self.assertEqual(counts["GET"], 4)
            self.assertEqual(counts["body_bytes"], len(expected) + 3)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_process_resources_are_per_child(self):
        def run(megabytes):
            command = [
                sys.executable,
                "-c",
                f"import json; data=bytearray({megabytes}*1024*1024); print(json.dumps({{'ok':True}}))",
            ]
            return isolated(command, os.environ.copy(), 10)

        large = run(64)
        small = run(1)
        self.assertTrue(small["ok"])
        self.assertGreater(
            large["process"]["peak_rss_bytes"],
            small["process"]["peak_rss_bytes"] + 32 * 1024**2,
        )
        self.assertGreater(small["process"]["wall_s"], 0)

    def test_failed_workload_is_not_a_sample(self):
        with self.assertRaisesRegex(RuntimeError, "workload failed"):
            isolated(
                [sys.executable, "-c", "raise SystemExit(7)"], os.environ.copy(), 10
            )


if __name__ == "__main__":
    unittest.main()
