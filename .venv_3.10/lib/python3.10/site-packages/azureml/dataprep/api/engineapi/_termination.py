"""
This module provides helper method to analyzed reasons for process termination.
"""

import signal
import subprocess
import sys
from enum import Enum


class IsOOMKill(Enum):
    YES = 1
    NO = 2
    MAYBE = 3


def check_process_oom_killed(process: subprocess.Popen) -> IsOOMKill:
    """
    Check to see if this process exited successfully. In case of a positive non-zero exit code.

    :param process: the process object from subprocess.Popen
    :return: IsOOMKill
    """
    returncode = process.returncode

    if returncode < 0:
        # Note: negative return codes only occur on POSIX platforms and are caused by unhandled signals
        errorcode = -returncode
        errorname = signal.Signals(errorcode).name

        if sys.platform == 'linux' and errorcode == signal.SIGKILL:
            # On Linux, the kernel memory allocator overcommits memory by default. If we attempt to
            # actually use all that memory, then the OOM killer will kick in and send SIGKILL. We
            # have to check the kernel logs to see if this is the case.
            if _check_linux_oom_killed(process.pid):
                return IsOOMKill.YES
            return IsOOMKill.MAYBE

        if errorname in ['SIGKILL', 'SIGABRT']:
            return IsOOMKill.MAYBE

    return IsOOMKill.NO


def _check_linux_oom_killed(pid: int) -> bool:
    """
    Check to see if the Linux out of memory killer sent SIGKILL to this process. Raise an exception if killed by OOM.

    :param pid: process pid
    :return: bool
    """
    oom_killed = False
    try:
        out = subprocess.run(['dmesg', '-l', 'err'],
                             stdout=subprocess.PIPE, universal_newlines=True)
        log_lines = out.stdout.strip().lower().split('\n')
        for line in log_lines:
            if 'out of memory: kill process {}'.format(pid) in line:
                oom_killed = True
                break
    except Exception:
        # may not have permission to run dmesg to view syslog
        pass

    return oom_killed
