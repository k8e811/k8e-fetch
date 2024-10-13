#!/bin/bash
set -Eeuo pipefail
zero=$(readlink -f "$0")
zerodir="${zero%/*}"
cfg="${FETCHER_CONFIG:-${zerodir}/cfg.yaml}"
opwd="$PWD"
datadir="${FETCHER_DATADIR:-${opwd}}"
versionsdir="${FETCHER_VERSIOSNDIR:-${datadir}/versions}"
Main() {
    local group="${1:-current_cycle}"
    local buckets=()
    readarray -t buckets < <(yq -r ".fetch.$group | keys().[]" < "$cfg")
    FetchVersions "$versionsdir" "${buckets[@]}"
    exit 0
}
FetchVersions() {
    local vfile bucket vdir="$1"; shift
    mkdir -p "$vdir"
    echo '*' > "$vdir/.gitignore"
    echo "$@"
    if [ -n "${FETCHER_SKIP_VERSIONS:-}" ]; then return 0; fi
    for bucket in "$@"; do
        vfile="$vdir/$bucket.json"
        aws s3api list-object-versions --no-sign-request --bucket "$bucket" > "$vfile"
    done
}
Main "$@"
# shellcheck disable=SC2317
exit 1