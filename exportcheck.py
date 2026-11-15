"""Independent fixed business expectations for the provided export."""
import copy
import io
import json
import sys
from checks import MemoryBlobs, assert_results, fixtures, normal as business
from exporter import stream
from pipeline import Engine, connect


class Output(io.StringIO):
    def __init__(self):
        super().__init__()
        self.flushes = 0

    def flush(self):
        self.flushes += 1


def normal(demo=False):
    db = connect(":memory:")
    engine = Engine(db, MemoryBlobs())
    for batch in fixtures():
        engine.accept(batch)
    expected = engine.drain()
    assert_results(expected)
    pending = copy.deepcopy(fixtures()[0])
    pending["batch"] = "unfinished"
    engine.accept(pending)
    out = Output()
    events = []
    stream(db, out, lambda event, detail: events.append(event))
    result = [json.loads(line) for line in out.getvalue().splitlines()]
    assert result == expected
    assert_results(result)
    assert [item["batch"] for item in result] == ["a", "b"]
    assert out.flushes >= 2 and out.getvalue().endswith("\n")
    assert not db.in_transaction
    assert events == ["export_opened", "export_streaming", "export_releasing"]
    empty = connect(":memory:")
    Engine(empty, MemoryBlobs())
    output = Output()
    stream(empty, output)
    assert output.getvalue() == "" and not empty.in_transaction
    empty.close()
    db.close()
    if demo:
        for item in result:
            print(json.dumps(item, ensure_ascii=False, sort_keys=True))
    else:
        print("EXPORT_BUSINESS_OK: ordered complete batches, original receipts, pending excluded, empty stream")


if __name__ == "__main__":
    demo = len(sys.argv) > 1 and sys.argv[1] == "demo"
    if not demo:
        business()
    normal(demo)
