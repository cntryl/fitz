from pathlib import Path
import os
import re
import subprocess
import sys
import tempfile
import unittest


class CiPipelineFailureTests(unittest.TestCase):
    def test_should_propagate_s3_campaign_failure_while_retaining_its_log(self):
        workflow = (Path(__file__).parents[1] / "workflows/ci.yml").read_text()
        step = workflow.split("      - name: Qualify S3 WAL retention and crash recovery at the resource floor\n", 1)[1]
        step = step.split("\n      - name:", 1)[0]
        settings, body = step.split("        run: |\n", 1)
        command = "\n".join(line.removeprefix("          ") for line in body.splitlines())
        # Match Actions' default shell versus explicit `shell: bash` invocation.
        shell = ["bash", "--noprofile", "--norc", "-e"]
        if re.search(r"^        shell: bash$", settings, re.MULTILINE):
            shell += ["-o", "pipefail"]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cargo = root / "cargo"
            cargo.write_text(f"#!{sys.executable}\nprint('campaign failure evidence')\nraise SystemExit(17)\n")
            cargo.chmod(0o755)
            result = subprocess.run(shell + ["-c", command], cwd=root,
                                    env=dict(os.environ, PATH=str(root) + os.pathsep + os.environ["PATH"]),
                                    capture_output=True, text=True, check=False)
            log = root / "target/fitz-stress/s3-recovery/campaign.log"
            self.assertEqual(log.read_text(), "campaign failure evidence\n")
            self.assertEqual(result.returncode, 17, "tee must not hide a failed S3 qualification")


if __name__ == "__main__":
    unittest.main()
