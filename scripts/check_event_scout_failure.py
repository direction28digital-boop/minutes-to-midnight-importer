"""Does the scout tell the truth about whether it actually ran?

The bug: every failure path in scout_hub returned [], so main() summed zeros,
printed "Done. 0 found", and exited 0. The workflow has an `if: failure()`
alert step -- it never fired, because the sensor never tripped. A run where the
Anthropic credit balance was empty looked exactly like a quiet week.

Each case stubs requests.post and asserts the EXIT CODE, which is the only
thing GitHub Actions reads.
"""
import io
import json
import sys
import contextlib
import tempfile
from pathlib import Path

# Run from the repo root:  python3 scripts/check_event_scout_failure.py
sys.path.insert(0, str(Path(__file__).resolve().parent))
import event_scout  # noqa: E402

HUBS = [
    {"hubId": "phoenix-az", "city": "Phoenix", "regionCode": "AZ", "countryCode": "US"},
    {"hubId": "denver-co", "city": "Denver", "regionCode": "CO", "countryCode": "US"},
    {"hubId": "austin-tx", "city": "Austin", "regionCode": "TX", "countryCode": "US"},
]

GOOD_EVENT = {
    "title": "Yappy Hour at Wag Brewing",
    "url": "https://wagbrewing.example/yappy",
    "startDateTime": "2099-09-12T17:00:00",
    "venueName": "Wag Brewing Co",
    "addressLine1": "412 E Roosevelt St, Phoenix, AZ",
    "description": "A warm evening on the patio with your pup.",
    "dogPolicy": "dogs-welcome",
    "dogEvidence": "Leashed dogs are welcome on the patio all evening long.",
}


class Resp:
    def __init__(self, status, payload=None, text=""):
        self.status_code = status
        self._payload = payload
        self.text = text or json.dumps(payload or {})

    def json(self):
        return self._payload


def ok_with_events():
    return Resp(200, {
        "stop_reason": "end_turn",
        "content": [{"type": "text", "text": "Here you go:\n" + json.dumps([GOOD_EVENT])}],
    })


def ok_but_empty_city():
    """A real answer: the model searched and found nothing worth submitting."""
    return Resp(200, {
        "stop_reason": "end_turn",
        "content": [{"type": "text",
                     "text": "I searched thoroughly and found no qualifying dog events."}],
    })


def credit_exhausted():
    return Resp(400, text='{"error":{"message":"Your credit balance is too low"}}')


def unauthorized():
    return Resp(401, text='{"error":{"message":"invalid x-api-key"}}')


def truncated():
    return Resp(200, {
        "stop_reason": "max_tokens",
        "content": [{"type": "text", "text": "[{\"title\": \"Half an ev"}],
    })


def malformed_json():
    return Resp(200, {
        "stop_reason": "end_turn",
        "content": [{"type": "text", "text": "[{'title': not valid json,,,}]"}],
    })


def run(responder, hubs=HUBS):
    """Run main() with requests.post stubbed. Returns (exit_code, stdout)."""
    # A temp file, not one written beside this script. A check that leaves
    # a stray _hubs.json in scripts/ is a check that dirties the tree.
    tmp = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
    json.dump(hubs, tmp)
    tmp.close()
    hubs_file = Path(tmp.name)

    calls = {"n": 0}

    def fake_post(*_a, **_kw):
        calls["n"] += 1
        return responder(calls["n"])

    real_post, real_argv = event_scout.requests.post, sys.argv
    event_scout.requests.post = fake_post
    sys.argv = ["event_scout.py", "--dry-run", "--hubs-file", str(hubs_file)]
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
            code = event_scout.main()
    except SystemExit as e:
        code = e.code if isinstance(e.code, int) else 1
    finally:
        event_scout.requests.post, sys.argv = real_post, real_argv
    return code, buf.getvalue()


CASES = [
    ("all cities answer, events found",
     lambda n: ok_with_events(), 0,
     "the happy path must stay exit 0"),

    ("all cities answer, genuinely nothing on",
     lambda n: ok_but_empty_city(), 0,
     "a quiet week is a REAL answer and must not alert, or the alert becomes noise and gets muted"),

    ("every city fails, credit balance empty",
     lambda n: credit_exhausted(), 1,
     "THE BUG. This is what actually happened. Old code: exit 0, green tick, nobody told."),

    ("every city fails, bad API key",
     lambda n: unauthorized(), 1,
     "same shape as the credit case and equally invisible before"),

    ("partial: 1 of 3 cities fails",
     lambda n: unauthorized() if n == 2 else ok_with_events(), 1,
     "a sweep that silently lost a third of its cities must not look like a full sweep"),

    ("reply truncated at max_tokens",
     lambda n: truncated(), 1,
     "a cut-off reply is a failed search, not an empty city"),

    ("reply is malformed JSON",
     lambda n: malformed_json(), 1,
     "this path printed NOTHING at all before, the quietest of the three"),
]

failures = 0
for name, responder, want, why in CASES:
    code, out = run(responder)
    ok = code == want
    if not ok:
        failures += 1
    print(f"{'PASS' if ok else 'FAIL'}  exit {code} (want {want})  {name}")
    if not ok:
        print(f"      why it matters: {why}")
        print("      ---- output ----")
        print("      " + out.replace("\n", "\n      ")[-1200:])

print()
print(f"{len(CASES) - failures}/{len(CASES)} cases behave correctly.")

# Show the operator-facing message for the case that actually bit her.
code, out = run(lambda n: credit_exhausted())
print("\n--- what the log now says when the credits run out ---")
print("\n".join(out.strip().splitlines()[-8:]))

sys.exit(1 if failures else 0)
