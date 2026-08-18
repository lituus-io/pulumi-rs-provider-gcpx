# Copyright: lituus-io, all rights reserved.
# Author: terekete <spicyzhug@gmail.com>

"""Console entry point.

Pulumi launches the plugin as a subprocess and reads a port from its stdout, so
this hands the process over rather than wrapping it: anything that buffers or
rewrites stdout would break the handshake.
"""

import os
import subprocess
import sys


def main() -> None:
    from pulumi_rs_provider_gcpx._find_binary import find_binary

    binary = find_binary()
    if sys.platform == "win32":
        # Windows has no exec that replaces the process, so the return code is
        # forwarded instead.
        sys.exit(subprocess.run([binary, *sys.argv[1:]], check=False).returncode)
    os.execv(binary, [binary, *sys.argv[1:]])
