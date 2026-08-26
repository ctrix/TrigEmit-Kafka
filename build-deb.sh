#!/bin/bash
#
# Builds the Debian package.
#
#   ./build-deb.sh
#
# debian/ is generated, not tracked: this copies the template from
# debian-build/ and writes debian/changelog with a version derived from git,
# the same way cmake/VersionString.cmake derives the version compiled into the
# module. That keeps the package version and the one kafka_info() reports
# telling the same story instead of drifting apart.
#
# Artifacts are collected in dist/.
#

set -euo pipefail

cd "$(dirname "$0")"

TEMPLATE="debian-build"
DISTDIR="dist"

if [ ! -d "$TEMPLATE" ]; then
    echo "$0: no $TEMPLATE/ to build from" >&2
    exit 1
fi

#
# Turn 'git describe' into something dpkg accepts.
#
# A Debian upstream version has to start with a digit, and in a native package
# it cannot contain a hyphen -- dpkg reads a hyphen as the start of the Debian
# revision. So the tag keeps its dots, the commit distance and hash become a
# '+' suffix, and a leading 'v' is dropped:
#
#   v0.1.0                  -> 0.1.0
#   v0.1.0-3-g1a2b3c4       -> 0.1.0+3.g1a2b3c4
#   1a2b3c4   (no tag)      -> 0.0.0+git.1a2b3c4
#   ...-dirty               -> ...+dirty
#
# The '+' suffixes sort above the bare tag, so a package built after a release
# upgrades over the release itself, which is what you want from a dev build.
#
describe="$(git describe --tags --always --dirty 2>/dev/null || true)"

if [ -z "$describe" ]; then
    echo "$0: no git information, cannot derive a version" >&2
    exit 1
fi

dirty=""
case "$describe" in
    *-dirty)
        dirty="+dirty"
        describe="${describe%-dirty}"
        ;;
esac

if [[ "$describe" =~ ^v?([0-9][0-9.]*)-([0-9]+)-g([0-9a-f]+)$ ]]; then
    # Commits after a tag
    version="${BASH_REMATCH[1]}+${BASH_REMATCH[2]}.g${BASH_REMATCH[3]}"
elif [[ "$describe" =~ ^v?([0-9][0-9.]*)$ ]]; then
    # Exactly on a tag
    version="${BASH_REMATCH[1]}"
else
    # No tag reachable: describe is a bare commit hash
    version="0.0.0+git.${describe}"
fi

version="${version}${dirty}"

# One place for the maintainer: the control file the changelog must agree with
maintainer="$(sed -n 's/^Maintainer: //p' "$TEMPLATE/control")"
if [ -z "$maintainer" ]; then
    echo "$0: no Maintainer in $TEMPLATE/control" >&2
    exit 1
fi

echo "Version: $version  ($describe$dirty)"

rm -rf debian
cp -r "$TEMPLATE" debian

cat > debian/changelog <<EOF
trigemit-kafka ($version) unstable; urgency=medium

  * Build from $describe$dirty.

 -- $maintainer  $(date -R)
EOF

dpkg-buildpackage -us -uc -b

#
# dpkg-buildpackage drops its output next to the source tree. Collect it, so a
# build does not litter the parent directory.
#
mkdir -p "$DISTDIR"
for f in ../trigemit-kafka*_"${version}"_*.deb \
         ../trigemit-kafka*_"${version}"_*.buildinfo \
         ../trigemit-kafka*_"${version}"_*.changes; do
    [ -e "$f" ] && mv -f "$f" "$DISTDIR/"
done

echo
echo "Built:"
ls -1 "$DISTDIR"/*"${version}"* 2>/dev/null || true
