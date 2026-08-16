from __future__ import annotations

from pathlib import Path


class ChunkedLogWriter:
    def __init__(self, result_dir: Path, log_name: str, max_file_bytes: int) -> None:
        self.result_dir = result_dir
        self.log_name = log_name
        self.max_file_bytes = max_file_bytes
        self.part_index = 0
        self.current_file: Path | None = None
        self.current_size = 0
        self.result_dir.mkdir(parents=True, exist_ok=True)
        for old_part in self.result_dir.glob(f"{self.log_name}.part*.txt"):
            old_part.unlink()
        self._open_next()

    def write_line(self, line: str) -> None:
        data = line.encode("utf-8", errors="replace")
        start = 0
        while start < len(data):
            remaining = self.max_file_bytes - self.current_size
            if remaining <= 0:
                self._open_next()
                remaining = self.max_file_bytes

            chunk = data[start : start + remaining]
            if self.current_file is None:
                raise RuntimeError("log writer has no open file")
            with self.current_file.open("ab") as handle:
                handle.write(chunk)
            self.current_size += len(chunk)
            start += len(chunk)

    def written_paths(self) -> list[Path]:
        return sorted(self.result_dir.glob(f"{self.log_name}.part*.txt"))

    def _open_next(self) -> None:
        self.current_file = self.result_dir / f"{self.log_name}.part{self.part_index:04d}.txt"
        self.current_file.write_bytes(b"")
        self.current_size = 0
        self.part_index += 1
