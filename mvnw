#!/bin/sh
# Apache Maven Wrapper bootstrap script (only-script distribution).
set -eu

BASE_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROPERTIES="$BASE_DIR/.mvn/wrapper/maven-wrapper.properties"

read_property() {
  sed -n "s/^$1=//p" "$PROPERTIES" | tr -d '\r' | tail -n 1
}

DISTRIBUTION_URL=$(read_property distributionUrl)
DISTRIBUTION_SHA256=$(read_property distributionSha256Sum)
[ -n "$DISTRIBUTION_URL" ] || { echo "Missing distributionUrl in $PROPERTIES" >&2; exit 1; }

ARCHIVE_NAME=${DISTRIBUTION_URL##*/}
DIST_DIR_NAME=${ARCHIVE_NAME%-bin.zip}
MAVEN_USER_HOME=${MAVEN_USER_HOME:-"$HOME/.m2"}
URL_HASH=$(printf '%s' "$DISTRIBUTION_URL" | cksum | awk '{print $1}')
MAVEN_HOME="$MAVEN_USER_HOME/wrapper/dists/$DIST_DIR_NAME/$URL_HASH/$DIST_DIR_NAME"

if [ ! -x "$MAVEN_HOME/bin/mvn" ]; then
  TMP_DIR=$(mktemp -d)
  trap 'rm -rf "$TMP_DIR"' EXIT HUP INT TERM
  ARCHIVE="$TMP_DIR/$ARCHIVE_NAME"
  mkdir -p "$(dirname "$MAVEN_HOME")"

  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$DISTRIBUTION_URL" -o "$ARCHIVE"
  elif command -v wget >/dev/null 2>&1; then
    wget -q "$DISTRIBUTION_URL" -O "$ARCHIVE"
  else
    echo "Neither curl nor wget is available to download Maven." >&2
    exit 1
  fi

  if [ -n "$DISTRIBUTION_SHA256" ]; then
    if command -v sha256sum >/dev/null 2>&1; then
      printf '%s  %s\n' "$DISTRIBUTION_SHA256" "$ARCHIVE" | sha256sum -c - >/dev/null
    elif command -v shasum >/dev/null 2>&1; then
      printf '%s  %s\n' "$DISTRIBUTION_SHA256" "$ARCHIVE" | shasum -a 256 -c >/dev/null
    else
      echo "Cannot validate Maven distribution checksum: sha256sum/shasum unavailable." >&2
      exit 1
    fi
  fi

  unzip -q "$ARCHIVE" -d "$TMP_DIR"
  mv "$TMP_DIR/$DIST_DIR_NAME" "$MAVEN_HOME"
fi

exec "$MAVEN_HOME/bin/mvn" "$@"
