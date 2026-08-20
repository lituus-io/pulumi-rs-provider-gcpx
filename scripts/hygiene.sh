#!/usr/bin/env bash
# Copyright: lituus-io, all rights reserved.
# Author: terekete <spicyzhug@gmail.com>
#
# Fails if vendor or downstream names appear anywhere in the tree or in commit
# messages. These names belong to other organisations and must not enter this
# repository's source or history.

set -euo pipefail

# Assembled at runtime so this script does not itself contain the literals it bans.
FORBIDDEN='(^|[^a-z])(bli)([^a-z]|$)|anthropic|cl'"aude"'|te'"lus"'|mark\.gates'

fail=0

# Only tracked text files: those are what ships and what the history carries.
# Scanning the working tree instead picks up build output — a compiled binary
# will match almost any short token by accident — and reports a failure that
# says nothing about the source.
echo "==> Scanning tracked files"
if matches=$(git ls-files -z \
      | grep -zZv '^scripts/hygiene\.sh$' \
      | xargs -0 grep -IniE "$FORBIDDEN" 2>/dev/null); then
  echo "FAIL: forbidden token(s) found in tree:"
  echo "$matches"
  fail=1
fi

echo "==> Scanning commit messages"
if [ -d .git ]; then
  if matches=$(git log --format='%H %s%n%b' | grep -niE "$FORBIDDEN" 2>/dev/null); then
    echo "FAIL: forbidden token(s) found in commit messages:"
    echo "$matches"
    fail=1
  fi
fi

if [ "$fail" -eq 0 ]; then
  echo "OK: no forbidden tokens."
fi
exit "$fail"
