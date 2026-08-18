# Copyright: lituus-io, all rights reserved.
# Author: terekete <spicyzhug@gmail.com>

"""Packaging wrapper for the gcpx Pulumi provider.

The wheel exists to deliver a compiled plugin binary through a Python
dependency. It contains no provider logic: everything lives in the binary,
and this package's only job is to say where it is.
"""

from pulumi_rs_provider_gcpx._find_binary import find_binary

__all__ = ["find_binary", "__version__"]
__version__ = "0.1.0"
