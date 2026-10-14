"""Static checks on the Step Functions definition and Terraform, so a broken
state machine or HCL syntax error fails CI before anyone runs `terraform apply`."""
import json
import re
from pathlib import Path

import hcl2

ROOT = Path(__file__).resolve().parents[1]
ASL = ROOT / "stepfunctions" / "pipeline.asl.json"
TF = ROOT / "infra" / "terraform"


def resources(path):
    """Map resource type -> {name: body}. Newer python-hcl2 versions keep the quotes on labels."""
    out = {}
    for block in hcl2.load(open(path)).get("resource", []):
        for rtype, named in block.items():
            for name, body in named.items():
                out.setdefault(rtype.strip('"'), {})[name.strip('"')] = body
    return out


def test_state_machine_is_valid_json_with_reachable_states():
    definition = json.loads(ASL.read_text())
    states = definition["States"]
    assert definition["StartAt"] in states
    targets = set()
    for s in states.values():
        targets |= {v for k, v in s.items() if k in ("Next", "Default")}
        targets |= {c["Next"] for c in s.get("Choices", [])}
        targets |= {c["Next"] for c in s.get("Catch", [])}
    assert targets <= set(states), f"unknown targets: {targets - set(states)}"
    unreachable = set(states) - targets - {definition["StartAt"]}
    assert not unreachable, f"unreachable states: {unreachable}"


def test_every_task_failure_alerts():
    states = json.loads(ASL.read_text())["States"]
    for name in ("RunGlueTransform", "LoadRedshift"):
        assert states[name]["Catch"][0]["Next"] == "NotifyFailure"


def test_template_placeholders_match_terraform_variables():
    placeholders = set(re.findall(r"\$\{(\w+)\}", ASL.read_text()))
    sfn = resources(TF / "orchestration.tf")["aws_sfn_state_machine"]["pipeline"]
    provided = set(re.findall(r"(\w+)\s*=", str(sfn["definition"])))
    assert placeholders <= provided, f"missing template vars: {placeholders - provided}"


def test_terraform_files_parse():
    files = list(TF.glob("*.tf"))
    assert len(files) >= 7
    for f in files:
        hcl2.load(open(f))


def test_buckets_block_public_access_and_encrypt():
    kinds = set(resources(TF / "s3.tf"))
    assert {"aws_s3_bucket_public_access_block", "aws_s3_bucket_server_side_encryption_configuration"} <= kinds
