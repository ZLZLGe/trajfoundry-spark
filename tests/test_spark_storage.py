from __future__ import annotations

import hashlib
import io
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import Mock

import pytest

from trajfoundry.credentials import CredentialFileError, S3Credentials
from trajfoundry.spark import storage
from trajfoundry.spark.output import SparkObjectDescription
from trajfoundry.spark.storage import create_output_store, local_output_path


def _forbidden_client(*args: Any, **kwargs: Any) -> None:
    raise AssertionError("local output must not construct an S3 client")


def _local(path: Path):
    return create_output_store(str(path), client_factory=_forbidden_client)


@pytest.mark.parametrize("uri", ["/tmp/traj-output", "file:///tmp/traj-output"])
def test_local_destination_parser(uri: str) -> None:
    assert local_output_path(uri) == Path("/tmp/traj-output")
    assert local_output_path("file:///tmp/a%20b/output/") == Path("/tmp/a b/output")
    assert local_output_path("s3://bucket/output/") is None


@pytest.mark.parametrize(
    "uri",
    [
        "",
        "relative/output",
        "~/output",
        "/",
        "file:///",
        "s3://bucket/",
        "file://host/tmp/output",
        "file://localhost/tmp/output",
        "file:/tmp/output",
        "https://host/output",
        "//host/output",
        "/tmp/../output",
        "/tmp/./output",
        "file:///tmp/%2e%2e/output",
        "file:///tmp/%00output",
        "file:////host/output",
        "/tmp/output?query",
        "/tmp/output#fragment",
        "file:///tmp/output?query",
        "file:///tmp/output#fragment",
        "/tmp/out\\put",
        "/tmp/out\nput",
    ],
)
def test_unsafe_destinations_rejected(uri: str) -> None:
    with pytest.raises(ValueError):
        local_output_path(uri)


def test_local_lifecycle_retry_and_no_s3(tmp_path: Path) -> None:
    root = tmp_path / "shared" / "run"
    driver = _local(root)
    driver.initialize_run("run-1")
    worker = create_output_store(
        root.as_uri(), client_factory=_forbidden_client, run_id="run-1"
    )
    first = worker.put_bytes("trajectory.jsonl", b"{}\n")
    assert first == worker.put_bytes("trajectory.jsonl", b"{}\n")
    assert first == SparkObjectDescription(
        "trajectory.jsonl", hashlib.sha256(b"{}\n").hexdigest(), 3
    )
    driver.verify(first)
    copy = tmp_path / "copied" / "part.jsonl"
    assert driver.copy_to(first.path, copy) == first
    assert copy.read_bytes() == b"{}\n"
    with pytest.raises(RuntimeError, match="only the driver"):
        worker.finish_run()
    driver.finish_run()
    assert sorted(path.name for path in root.iterdir()) == ["trajectory.jsonl"]
    with pytest.raises(RuntimeError, match="marker is missing"):
        worker.put_bytes("late.jsonl", b"{}\n")


def test_conflicts_and_nonempty_outputs_are_preserved(tmp_path: Path) -> None:
    root = tmp_path / "output"
    store = _local(root)
    store.initialize_run("first")
    store.put_bytes("trajectory.jsonl", b"original\n")
    with pytest.raises(FileExistsError, match="conflicting"):
        store.put_bytes("trajectory.jsonl", b"replacement\n")
    assert (root / "trajectory.jsonl").read_bytes() == b"original\n"
    with pytest.raises(ValueError, match="not empty"):
        _local(root).initialize_run("second")
    assert (root / ".trajfoundry-spark-run").read_text() == "first\n"
    assert not list(root.glob(".trajfoundry-write-*"))
    store.finish_run()
    with pytest.raises(ValueError, match="not empty"):
        _local(root).ensure_empty()


def test_uninitialized_or_unshared_worker_never_creates_outputs(tmp_path: Path) -> None:
    root = tmp_path / "missing"
    with pytest.raises(FileNotFoundError):
        create_output_store(
            str(root), run_id="driver-run", client_factory=_forbidden_client
        )
    assert not root.exists()
    root.mkdir()
    with pytest.raises(RuntimeError, match="marker is missing"):
        create_output_store(
            str(root), run_id="driver-run", client_factory=_forbidden_client
        )
    with pytest.raises(RuntimeError, match="initialize"):
        _local(root).put_bytes("data.jsonl", b"{}")
    assert not list(root.iterdir())


def test_mismatched_marker_blocks_writes_and_cleanup(tmp_path: Path) -> None:
    root = tmp_path / "output"
    store = _local(root)
    store.initialize_run("first")
    with pytest.raises(RuntimeError, match="another run"):
        create_output_store(
            str(root), run_id="second", client_factory=_forbidden_client
        )
    (root / ".trajfoundry-spark-run").write_text("different-run\n")
    with pytest.raises(RuntimeError, match="another run"):
        store.put_bytes("data.jsonl", b"{}")
    with pytest.raises(RuntimeError, match="another run"):
        store.finish_run()
    assert (root / ".trajfoundry-spark-run").read_text() == "different-run\n"


def test_streamed_file_put_copy_and_verification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from trajfoundry.spark import storage

    monkeypatch.setattr(storage, "_CHUNK_BYTES", 7)
    payload = b"a streamed output\n" * 100
    source = tmp_path / "source.jsonl"
    source.write_bytes(payload)
    store = _local(tmp_path / "output")
    store.initialize_run("streamed")

    def forbidden_read_bytes(path: Path) -> bytes:
        raise AssertionError("whole-file read is forbidden")

    monkeypatch.setattr(Path, "read_bytes", forbidden_read_bytes)
    description = store.put_file("trajectory.jsonl", source)
    assert description.bytes == len(payload)
    assert description.sha256 == hashlib.sha256(payload).hexdigest()
    store.verify(description)
    assert store.copy_to(description.path, tmp_path / "copy.jsonl") == description
    with (tmp_path / "copy.jsonl").open("rb") as handle:
        assert handle.read() == payload
    (tmp_path / "output" / description.path).write_bytes(b"corrupted")
    with pytest.raises(ValueError, match="hash or size"):
        store.verify(description)


@pytest.mark.parametrize(
    "name",
    [
        "",
        ".",
        "..",
        "../escape",
        "/escape",
        "a/b",
        "a\\b",
        "a\x00b",
        ".trajfoundry-spark-run",
        ".trajfoundry-write-any",
    ],
)
def test_flat_name_guards(tmp_path: Path, name: str) -> None:
    store = _local(tmp_path / "output")
    store.initialize_run("test")
    for operation in (
        lambda: store.put_bytes(name, b"x"),
        lambda: store.put_file(name, tmp_path / "source"),
        lambda: store.copy_to(name, tmp_path / "copy"),
        lambda: store.delete(name),
    ):
        with pytest.raises(ValueError, match="flat"):
            operation()


def test_symlink_directories_files_and_markers_are_refused(tmp_path: Path) -> None:
    actual = tmp_path / "actual"
    actual.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(actual, target_is_directory=True)
    with pytest.raises(OSError):
        _local(alias / "output").initialize_run("run")
    assert not list(actual.iterdir())
    store = _local(tmp_path / "output")
    store.initialize_run("run")
    sensitive = tmp_path / "keep.txt"
    sensitive.write_bytes(b"preserve")
    link = tmp_path / "output" / "link.jsonl"
    link.symlink_to(sensitive)
    for operation in (
        lambda: store.put_bytes(link.name, b"replacement"),
        lambda: store.delete(link.name),
        lambda: store.copy_to(link.name, tmp_path / "copy"),
    ):
        with pytest.raises(OSError):
            operation()
    assert sensitive.read_bytes() == b"preserve"
    marker = tmp_path / "output" / ".trajfoundry-spark-run"
    marker.unlink()
    marker.symlink_to(sensitive)
    with pytest.raises(OSError):
        store.finish_run()
    assert marker.is_symlink()


def test_delete_is_scoped_and_idempotent(tmp_path: Path) -> None:
    store = _local(tmp_path / "output")
    store.initialize_run("run")
    store.put_bytes("part.jsonl", b"part")
    store.put_bytes("keep.jsonl", b"keep")
    store.delete("part.jsonl")
    store.delete("part.jsonl")
    assert (tmp_path / "output" / "keep.jsonl").read_bytes() == b"keep"
    assert (tmp_path / "output" / ".trajfoundry-spark-run").exists()


@pytest.mark.parametrize(
    "name", ["same.jsonl", "diagnostics/._objects_part_000001.ndjson"]
)
def test_concurrent_identical_retry_is_atomic(tmp_path: Path, name: str) -> None:
    root = tmp_path / "output"
    driver = _local(root)
    driver.initialize_run("parallel")

    def publish(_: int) -> SparkObjectDescription:
        worker = create_output_store(
            str(root), run_id="parallel", client_factory=_forbidden_client
        )
        return worker.put_bytes(name, b"deterministic\n" * 1000)

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(publish, range(16)))
    assert all(description == results[0] for description in results)
    driver.verify(results[0])
    assert not list(root.rglob(".trajfoundry-write-*"))


def test_interrupted_stream_never_publishes_partial_file(tmp_path: Path) -> None:
    from trajfoundry.spark.storage import _atomic_write, _directory_fd

    class BrokenStream(io.BytesIO):
        def read(self, size: int = -1) -> bytes:
            if self.tell():
                raise OSError("injected read failure")
            return super().read(4)

    with (
        _directory_fd(tmp_path) as directory,
        pytest.raises(OSError, match="injected"),
    ):
        _atomic_write(directory, "output.jsonl", BrokenStream(b"abcdefgh"))
    assert not list(tmp_path.iterdir())


def test_only_one_initializer_claims_empty_directory(tmp_path: Path) -> None:
    root = tmp_path / "output"
    root.mkdir()

    def initialize(index: int) -> str | None:
        run_id = f"run-{index}"
        try:
            _local(root).initialize_run(run_id)
        except (ValueError, FileExistsError):
            return None
        return run_id

    with ThreadPoolExecutor(max_workers=4) as pool:
        winners = [result for result in pool.map(initialize, range(8)) if result]
    assert len(winners) == 1
    assert (root / ".trajfoundry-spark-run").read_text() == winners[0] + "\n"


class _MissingObject(Exception):
    response: ClassVar = {"Error": {"Code": "NoSuchKey"}}


class _S3:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.metadata: dict[str, dict[str, str]] = {}
        self.puts = 0
        self.body: io.BytesIO | None = None

    def get_paginator(self, name: str):
        assert name == "list_objects_v2"
        return self

    def paginate(self, **kwargs: Any):
        yield {
            "Contents": [
                {"Key": key} for key in self.objects if key.startswith(kwargs["Prefix"])
            ]
        }

    def head_object(self, *, Bucket: str, Key: str):
        if Key not in self.objects:
            raise _MissingObject
        return {"ContentLength": len(self.objects[Key]), "Metadata": self.metadata[Key]}

    def put_object(
        self, *, Bucket: str, Key: str, Body: Any, Metadata: dict, **kwargs: Any
    ):
        self.puts += 1
        self.objects[Key] = Body if isinstance(Body, bytes) else Body.read()
        self.metadata[Key] = Metadata

    def get_object(self, *, Bucket: str, Key: str):
        self.body = io.BytesIO(self.objects[Key])
        return {"Body": self.body}

    def delete_object(self, *, Bucket: str, Key: str):
        self.objects.pop(Key, None)


def test_s3_wrapper_preserves_existing_put_contract(tmp_path: Path) -> None:
    client = _S3()
    store = create_output_store(
        "s3://bucket/output/", client_factory=lambda *args: client
    )
    assert store.local_path is None
    store.initialize_run("run")
    description = store.put_bytes("data.jsonl", b"data\n")
    assert store.put_bytes("data.jsonl", b"data\n") == description
    assert client.puts == 1
    source = tmp_path / "source"
    source.write_bytes(b"streamed\n")
    streamed = store.put_file("streamed.jsonl", source)
    assert store.put_file("streamed.jsonl", source) == streamed
    assert client.puts == 2
    assert store.copy_to("streamed.jsonl", tmp_path / "copy") == streamed
    assert client.body is not None and client.body.closed
    store.verify(description)
    with pytest.raises(ValueError, match="not empty"):
        store.ensure_empty()
    store.delete("data.jsonl")
    assert "output/data.jsonl" not in client.objects
    store.finish_run()


def test_failed_s3_download_preserves_destination(tmp_path: Path) -> None:
    client = _S3()
    store = create_output_store(
        "s3://bucket/output/", client_factory=lambda *args: client
    )
    store.put_bytes("data.jsonl", b"new-data")
    target = tmp_path / "existing"
    target.write_bytes(b"old-data")
    with pytest.raises(FileExistsError):
        store.copy_to("data.jsonl", target)
    assert target.read_bytes() == b"old-data"
    assert client.body is not None and client.body.closed
    assert not list(tmp_path.glob(".trajfoundry-write-*"))


@pytest.mark.parametrize("as_file", [False, True])
def test_s3_conflicting_retry_preserves_existing_bytes(tmp_path, as_file):
    client = _S3()
    store = create_output_store("s3://bucket/output/", client_factory=lambda *_: client)
    original = store.put_bytes("data.jsonl", b"original\n")
    source = tmp_path / "source"
    source.write_bytes(b"replacement\n")
    with pytest.raises(FileExistsError, match="existing object was preserved"):
        if as_file:
            store.put_file("data.jsonl", source)
        else:
            store.put_bytes("data.jsonl", b"replacement\n")
    store.verify(original)
    assert client.puts == 1


@pytest.mark.parametrize("payload", [b"changed", b"short", b"too-long-data"])
def test_s3_verify_detects_corruption_even_with_original_metadata(payload):
    client = _S3()
    store = create_output_store("s3://bucket/output/", client_factory=lambda *_: client)
    description = store.put_bytes("data.jsonl", b"correct")
    client.objects["output/data.jsonl"] = payload
    with pytest.raises(ValueError, match="hash or size"):
        store.verify(description)
    assert client.body.closed


def test_s3_verification_stream_is_bounded_and_stops_at_expected_size(monkeypatch):
    client = _S3()
    store = create_output_store("s3://bucket/output/", client_factory=lambda *_: client)
    description = store.put_bytes("data.jsonl", b"1234567")
    reads = []

    class Body(io.BytesIO):
        def read(self, size=-1):
            assert 0 < size <= 3
            reads.append(size)
            return super().read(size)

    body = Body(b"1234567extra-data")
    monkeypatch.setattr(storage, "_CHUNK_BYTES", 3)
    monkeypatch.setattr(client, "get_object", lambda **_: {"Body": body})
    with pytest.raises(ValueError, match="hash or size"):
        store.verify(description)
    assert reads == [3, 3, 2]
    assert body.closed


def test_s3_verify_rejects_content_length_and_closes_body(monkeypatch):
    client = _S3()
    store = create_output_store("s3://bucket/output/", client_factory=lambda *_: client)
    description = store.put_bytes("data.jsonl", b"data")
    body = io.BytesIO(b"data")
    monkeypatch.setattr(
        client, "get_object", lambda **_: {"Body": body, "ContentLength": 9}
    )
    with pytest.raises(ValueError, match="hash or size"):
        store.verify(description)
    assert body.closed


def test_explicit_output_credentials_load_only_for_s3(monkeypatch, tmp_path):
    credentials = S3Credentials("synthetic-key", "synthetic-secret")
    loader = Mock(return_value=credentials)
    factory = Mock(return_value=_S3())
    monkeypatch.setattr(storage, "load_s3_credentials", loader)
    path = "/shared/private/output.json"
    create_output_store(
        str(tmp_path / "out"),
        client_factory=factory,
        output_credentials_path=path,
    )
    loader.assert_not_called()
    factory.assert_not_called()
    create_output_store(
        "s3://bucket/output/",
        client_factory=factory,
        output_credentials_path=path,
    )
    loader.assert_called_once_with(path)
    factory.assert_called_once_with(
        "https://s3.amazonaws.com", "us-east-1", credentials=credentials
    )


def test_default_output_credentials_preserve_sdk_chain(monkeypatch):
    loader = Mock(side_effect=AssertionError("no explicit credentials requested"))
    factory = Mock(return_value=_S3())
    monkeypatch.setattr(storage, "load_s3_credentials", loader)
    create_output_store("s3://bucket/output/", client_factory=factory)
    loader.assert_not_called()
    factory.assert_called_once_with("https://s3.amazonaws.com", "us-east-1")


@pytest.mark.parametrize("mode", [0o400, 0o600, 0o644])
def test_output_credentials_keep_secure_file_mode_policy(tmp_path, mode):
    path = tmp_path / "credentials.json"
    path.write_text(json.dumps({
        "aws_access_key_id": "synthetic-key",
        "aws_secret_access_key": "synthetic-secret",
    }))
    path.chmod(mode)
    factory = Mock(return_value=_S3())
    if mode == 0o644:
        with pytest.raises(CredentialFileError, match="unsafe"):
            create_output_store(
                "s3://bucket/output/",
                client_factory=factory,
                output_credentials_path=str(path),
            )
        factory.assert_not_called()
    else:
        create_output_store(
            "s3://bucket/output/",
            client_factory=factory,
            output_credentials_path=str(path),
        )
        assert factory.call_args.kwargs["credentials"] == S3Credentials(
            "synthetic-key", "synthetic-secret"
        )
