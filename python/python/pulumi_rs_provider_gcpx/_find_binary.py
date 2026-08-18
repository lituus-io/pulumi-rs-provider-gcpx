# Copyright: lituus-io, all rights reserved.
# Author: terekete <spicyzhug@gmail.com>

"""Locate the bundled plugin binary.

Downstream packaging imports :func:`find_binary` directly to put the binary on
PATH and to link it into Pulumi's plugin directory, so its name and signature
are a published interface rather than an internal detail.
"""

import os
import sysconfig

_BIN_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bin")

# Pulumi's plugin loader resolves a provider by this exact filename. It is not
# the distribution name and does not follow it.
BINARY_STEM = "pulumi-resource-gcpx"


def _exe_suffix() -> str:
    return sysconfig.get_config_var("EXE") or ""


def find_binary() -> str:
    """Return the absolute path of the bundled plugin binary.

    Raises:
        FileNotFoundError: if the wheel carries no binary for this platform,
            which means the wrong wheel was installed rather than that the
            binary is merely missing — so the message says so.
    """
    path = os.path.join(_BIN_DIR, BINARY_STEM + _exe_suffix())
    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"no provider binary at {path}. This usually means a wheel built "
            f"for another platform was installed; reinstall with "
            f"`pip install --force-reinstall pulumi-rs-provider-gcpx`."
        )
    return path
