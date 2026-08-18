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

echo "==> Scanning tree"
if matches=$(grep -rniE "$FORBIDDEN" . \
      --exclude-dir=.git \
      --exclude-dir=target \
      --exclude=hygiene.sh 2>/dev/null); then
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
