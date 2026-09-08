"""Gated stdio process owned by the existing resident supervisor.

Preparation does not execute application code. The owner records the original
PID/birth/Job before activation, owns business draining, and never interprets
an RPC timeout as process death. No session or model turn is created here.
"""
from __future__ import annotations

import os

from .app_server_rpc import StdioAppServerClient
from .windows_gated_process import start_suspended, observe_job


class AppServerProcess:
    def __init__(self, process, incoming, outgoing):
        self.process, self.incoming, self.outgoing = process, incoming, outgoing
        self.client = None
        self._disposed = False

    @classmethod
    def prepare(cls, *, command, cwd, environment, stderr_path):
        import msvcrt
        in_read, in_write = os.pipe()
        out_read, out_write = os.pipe()
        try:
            with stderr_path.open("wb") as errors:
                process = start_suspended(command, cwd=cwd, environment=environment,
                    stdio_handles=tuple(msvcrt.get_osfhandle(fd) for fd in (in_read, out_write, errors.fileno())))
        except BaseException:
            os.close(in_write)
            os.close(out_read)
            raise
        finally:
            os.close(in_read)
            os.close(out_write)
        return cls(process, os.fdopen(in_write, "w", encoding="utf-8"), os.fdopen(out_read, "r", encoding="utf-8"))

    def activate(self, *, on_notification=None):
        if self.client is not None or self._disposed:
            raise ValueError("original app-server process was already activated")
        self.client = StdioAppServerClient(self.incoming, self.outgoing, on_notification=on_notification)
        self.process.detach(keep_kill_on_close=True)
        self.process.resume()
        return self.client

    def request_shutdown(self):
        """The owner must first drain admitted turns; EOF is not a forced kill."""
        self.incoming.close()

    def dispose_exited(self, *, on_exit_observed=None):
        if self._disposed:
            return
        if self.process.poll() is None or observe_job(self.process.job_name) != "exited":
            raise ValueError("original app-server Job has not fully exited")
        if on_exit_observed is not None:
            on_exit_observed()  # Persist observed quiescence before closing the final Job handle.
        self.process.close()
        self.incoming.close()
        if self.client is not None:
            self.client._reader.join(timeout=5)
        self.outgoing.close()
        self._disposed = True

    def abort_prepared(self):
        if self.client is not None:
            raise ValueError("activated app-server requires original drain/exit observation")
        self.process.close()
        self.incoming.close()
        self.outgoing.close()
        self._disposed = True
