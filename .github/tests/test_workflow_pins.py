from pathlib import Path
import re
import unittest

WORKFLOWS = Path(__file__).parents[1] / "workflows"
USES = re.compile(r"^\s*(?:-\s*)?uses:\s*(\S+)(.*)$", re.M)
PINNED = re.compile(r"^[\w.-]+/[\w./-]+@[0-9a-f]{40}$")
VERSION_COMMENT = re.compile(r"^\s*# v\d+\.\d+\.\d+$")


def remote_uses():
    for path in sorted(WORKFLOWS.glob("*.y*ml")):
        for match in USES.finditer(path.read_text()):
            if not match.group(1).startswith("./"):
                yield path.name, match.group(1), match.group(2)


class WorkflowPinTests(unittest.TestCase):
    def test_should_pin_every_remote_action_to_a_full_commit_sha(self):
        unpinned = [(name, ref) for name, ref, _ in remote_uses() if not PINNED.match(ref)]
        self.assertEqual(unpinned, [])

    def test_should_name_the_exact_release_beside_every_pinned_action(self):
        missing = [(name, ref) for name, ref, rest in remote_uses() if not VERSION_COMMENT.match(rest)]
        self.assertEqual(missing, [])


if __name__ == "__main__":
    unittest.main()
