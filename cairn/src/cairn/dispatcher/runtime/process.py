from __future__ import annotations

from contextlib import suppress
from collections import deque
from dataclasses import dataclass
import json
import logging
import threading
import time
from typing import Any, Protocol, runtime_checkable

from docker.errors import APIError, DockerException
from docker.models.containers import Container

LOG = logging.getLogger(__name__)
EXEC_KILL_JOIN_TIMEOUT_SECONDS = 5.0
STDOUT_HEAD_LIMIT = 256 * 1024
STDOUT_TAIL_LIMIT = 8 * 1024 * 1024
STDERR_HEAD_LIMIT = 64 * 1024
STDERR_TAIL_LIMIT = 1024 * 1024
IMPORTANT_LINE_LIMIT = 1024 * 1024
TAIL_CHUNK_TARGET = 64 * 1024
TRUNCATION_MARKER = "\n[... cairn bounded process output omitted ...]\n"


class BoundedTextBuffer:
    def __init__(self, head_limit: int, tail_limit: int):
        self._head_limit = head_limit
        self._tail_limit = tail_limit
        self._head = ""
        self._tail: deque[str] = deque()
        self._tail_size = 0
        self._truncated = False

    @property
    def chunk_count(self) -> int:
        return len(self._tail)

    def append(self, value: str) -> None:
        if not value:
            return
        if len(self._head) < self._head_limit:
            take = min(self._head_limit - len(self._head), len(value))
            self._head += value[:take]
            value = value[take:]
        if not value:
            return
        if len(value) > self._tail_limit:
            self._truncated = True
            value = value[-self._tail_limit :]
        while value:
            if self._tail and len(self._tail[-1]) < TAIL_CHUNK_TARGET:
                take = min(TAIL_CHUNK_TARGET - len(self._tail[-1]), len(value))
                self._tail[-1] += value[:take]
                self._tail_size += take
                value = value[take:]
                continue
            chunk = value[:TAIL_CHUNK_TARGET]
            self._tail.append(chunk)
            self._tail_size += len(chunk)
            value = value[len(chunk) :]
        self._trim_tail()

    def value(self) -> str:
        tail = "".join(self._tail)
        if not self._truncated:
            return self._head + tail
        return self._head + TRUNCATION_MARKER + tail

    def __len__(self) -> int:
        return len(self._head) + self._tail_size

    def _trim_tail(self) -> None:
        while self._tail_size > self._tail_limit:
            overflow = self._tail_size - self._tail_limit
            first = self._tail[0]
            self._truncated = True
            if overflow and len(first) > overflow:
                self._tail[0] = first[overflow:]
                self._tail_size -= overflow
                continue
            self._tail.popleft()
            self._tail_size -= len(first)


class ImportantJsonLineBuffer:
    _TYPES = {"session", "turn_end", "agent_end"}

    def __init__(self, line_limit: int = IMPORTANT_LINE_LIMIT):
        self._line_limit = line_limit
        self._pending = ""
        self._discarding = False
        self._session: str | None = None
        self._completion: str | None = None

    def append(self, value: str) -> None:
        for part in value.splitlines(keepends=True):
            if self._discarding:
                if part.endswith(("\n", "\r")):
                    self._discarding = False
                continue
            if len(self._pending) + len(part) > self._line_limit:
                self._pending = ""
                self._discarding = not part.endswith(("\n", "\r"))
                continue
            self._pending += part
            if part.endswith(("\n", "\r")):
                self._capture(self._pending.strip())
                self._pending = ""

    def value(self) -> str:
        if self._pending:
            self._capture(self._pending.strip())
            self._pending = ""
        return "\n".join(line for line in (self._session, self._completion) if line)

    def _capture(self, line: str) -> None:
        if not line or '"type"' not in line:
            return
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            return
        event_type = payload.get("type") if isinstance(payload, dict) else None
        if event_type not in self._TYPES:
            return
        if event_type == "session" and self._session is None:
            self._session = line
        elif event_type in {"turn_end", "agent_end"}:
            self._completion = line


@dataclass(slots=True)
class ProcessResult:
    returncode: int
    stdout: str
    stderr: str
    timed_out: bool = False
    cancelled: bool = False
    cancel_reason: str | None = None


@runtime_checkable
class ExecProcess(Protocol):
    """A worker process, regardless of whether it runs inside a container or on the host.

    Container mode uses ManagedProcess; local mode uses LocalProcess. Both expose this
    surface so the task runners, heartbeat lease and cancellation stay backend-agnostic.
    """

    def start(self) -> None: ...

    def communicate(self, timeout: float | None) -> ProcessResult: ...

    def kill(self) -> None: ...

    def cancel(self, reason: str) -> None: ...


class ManagedProcess:
    def __init__(self, container: Container, command: list[str], env: dict[str, str]):
        self.command = command
        self.env = env
        self._container = container
        self._api = container.client.api
        self._exec_id: str | None = None
        self._reader: threading.Thread | None = None
        self._stdout = BoundedTextBuffer(STDOUT_HEAD_LIMIT, STDOUT_TAIL_LIMIT)
        self._stderr = BoundedTextBuffer(STDERR_HEAD_LIMIT, STDERR_TAIL_LIMIT)
        self._important_stdout = ImportantJsonLineBuffer()
        self._returncode: int | None = None
        self._timed_out = False
        self._cancel_reason: str | None = None
        self._read_error: str | None = None
        self._done = threading.Event()

    def start(self) -> None:
        exec_info = self._api.exec_create(
            self._container.id,
            self.command,
            stdout=True,
            stderr=True,
            stdin=False,
            tty=False,
            environment=self.env,
        )
        self._exec_id = exec_info["Id"]
        self._reader = threading.Thread(target=self._read_stream, daemon=True)
        self._reader.start()

    def communicate(self, timeout: float | None) -> ProcessResult:
        assert self._reader is not None
        self._reader.join(timeout=timeout)
        if self._reader.is_alive():
            self._timed_out = True
            self.kill()
            self._reader.join(timeout=EXEC_KILL_JOIN_TIMEOUT_SECONDS)
        if self._reader.is_alive():
            if self._returncode is None:
                self._returncode = 137
            self._done.set()
        self._done.wait(timeout=0)
        if self._read_error and not len(self._stderr):
            self._stderr.append(self._read_error)
        stdout = self._stdout.value()
        important = self._important_stdout.value()
        if important:
            stdout += "\n" + important + "\n"
        return ProcessResult(
            returncode=self._returncode if self._returncode is not None else 1,
            stdout=stdout,
            stderr=self._stderr.value(),
            timed_out=self._timed_out,
            cancelled=self._cancel_reason is not None,
            cancel_reason=self._cancel_reason,
        )

    def kill(self) -> None:
        if self._exec_id is None:
            return
        try:
            details = self._api.exec_inspect(self._exec_id)
        except DockerException as exc:
            LOG.warning("failed to inspect exec before kill exec_id=%s error=%s", self._exec_id, exc)
            return
        if not details.get("Running"):
            return
        pid = details.get("Pid")
        if not pid:
            LOG.warning("container exec missing pid for kill exec_id=%s", self._exec_id)
            return
        self._kill_pid(int(pid))

    def cancel(self, reason: str) -> None:
        if self._cancel_reason is None:
            self._cancel_reason = reason
        self.kill()

    def _read_stream(self) -> None:
        assert self._exec_id is not None
        stream: Any | None = None
        try:
            stream = self._api.exec_start(
                self._exec_id,
                detach=False,
                tty=False,
                stream=True,
                demux=True,
            )
            for chunk in stream:
                stdout, stderr = self._split_chunk(chunk)
                if stdout:
                    self._stdout.append(stdout)
                    self._important_stdout.append(stdout)
                if stderr:
                    self._stderr.append(stderr)
        except DockerException as exc:
            self._read_error = str(exc)
        finally:
            self._close_stream(stream)
            self._returncode = self._resolve_exit_code()
            self._done.set()

    @staticmethod
    def _close_stream(stream: Any | None) -> None:
        if stream is None:
            return
        close = getattr(stream, "close", None)
        if callable(close):
            with suppress(Exception):
                close()
        response = getattr(stream, "_response", None)
        response_close = getattr(response, "close", None)
        if callable(response_close):
            with suppress(Exception):
                response_close()

    def _resolve_exit_code(self) -> int:
        assert self._exec_id is not None
        deadline = time.monotonic() + EXEC_KILL_JOIN_TIMEOUT_SECONDS
        while True:
            try:
                details = self._api.exec_inspect(self._exec_id)
            except DockerException as exc:
                if self._read_error is None:
                    self._read_error = str(exc)
                return 137 if self._timed_out else 1
            exit_code = details.get("ExitCode")
            if exit_code is not None:
                return int(exit_code)
            if time.monotonic() >= deadline:
                return 137 if self._timed_out else 1
            time.sleep(0.1)

    def _kill_pid(self, pid: int) -> None:
        last_error: str | None = None
        for command in (
            ["kill", "-KILL", str(pid)],
            ["/bin/sh", "-lc", f"kill -KILL {pid}"],
            ["sh", "-lc", f"kill -KILL {pid}"],
        ):
            try:
                result = self._container.exec_run(command, stdout=False, stderr=False)
            except APIError as exc:
                last_error = str(exc)
                continue
            exit_code = result.exit_code if hasattr(result, "exit_code") else None
            if exit_code in (None, 0, 1):
                return
        if last_error is not None:
            LOG.warning("failed to kill container exec pid=%s container=%s error=%s", pid, self._container.name, last_error)

    @staticmethod
    def _split_chunk(chunk: Any) -> tuple[str, str]:
        if isinstance(chunk, tuple):
            stdout, stderr = chunk
        else:
            stdout, stderr = chunk, None
        return ManagedProcess._decode(stdout), ManagedProcess._decode(stderr)

    @staticmethod
    def _decode(chunk: bytes | str | None) -> str:
        if chunk is None:
            return ""
        if isinstance(chunk, bytes):
            return chunk.decode("utf-8", errors="replace")
        return chunk
