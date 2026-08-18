# Copyright: lituus-io, all rights reserved.
# Author: terekete <spicyzhug@gmail.com>

"""The packaging contract.

Downstream tooling installs this wheel and calls into it, so these assert the
shape that tooling depends on rather than anything about the provider itself.
"""

import os
import subprocess
import sys

import pytest

from pulumi_rs_provider_gcpx import __version__
from pulumi_rs_provider_gcpx._find_binary import BINARY_STEM, find_binary


def test_binary_is_present_and_executable():
    """The wheel must actually carry a binary, not merely a place for one."""
    path = find_binary()
    assert os.path.isfile(path), path
    assert os.access(path, os.X_OK), f"{path} is not executable"
    assert os.path.getsize(path) > 0, "binary is empty"


def test_binary_keeps_the_name_pulumi_looks_for():
    """Pulumi resolves a provider by filename, not by distribution name."""
    assert os.path.basename(find_binary()).startswith(BINARY_STEM)


def test_find_binary_is_importable_at_its_published_path():
    """Downstream packaging imports this exact symbol; moving it breaks them."""
    from pulumi_rs_provider_gcpx._find_binary import find_binary as imported

    assert callable(imported)


def test_version_matches_the_distribution():
    assert __version__ == "0.1.0"


@pytest.mark.skipif(sys.platform == "win32", reason="uses a POSIX process group")
def test_plugin_prints_a_port_before_anything_else():
    """The engine reads a port from stdout before connecting.

    Anything printed first — a banner, a warning, a credential error — breaks
    the handshake, so this asserts the very first line is a port number, with no
    credentials configured.
    """
    env = {**os.environ, "HOME": "/nonexistent"}
    env.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
    env.pop("CLOUDSDK_CONFIG", None)

    proc = subprocess.Popen(
        [find_binary()], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env=env, text=True, start_new_session=True,
    )
    try:
        first_line = proc.stdout.readline().strip()
        assert first_line.isdigit(), f"expected a port, got {first_line!r}"
        assert 1 <= int(first_line) <= 65535
    finally:
        proc.kill()
        proc.wait(timeout=10)
