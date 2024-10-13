#!/bin/bash
set -Eeuo pipefail
zero=$(readlink -f "$0")
zerodir="${zero%/*}"
cfg="${FETCHER_CONFIG:-${zerodir}/cfg.yaml}"
opwd="$PWD"
datadir="${FETCHER_DATADIR:-${opwd}}"
versionsdir="${FETCHER_VERSIONSDIR:-${datadir}/versions}"
syncdir="${FETCHER_SYNCDIR:-${datadir}/sync}"
Main() {
    local group="${1:-current_cycle}"
    local buckets=()
    readarray -t buckets < <(yq -r ".fetch.$group | keys().[]" < "$cfg")
    FetchVersions "$versionsdir" "${buckets[@]}"
    Sync "$syncdir" "$versionsdir" "$group"
    exit 0
}
FetchVersions() {
    local vfile bucket vdir="$1"; shift
    mkdir -pv "$vdir"
    echo '*' > "$vdir/.gitignore"
    echo "FetchVersions: $vdir," "$@"1>&2
    if [ -n "${FETCHER_SKIP_VERSIONS:-}" ]; then return 0; fi
    for bucket in "$@"; do
        vfile="$vdir/$bucket.json"
        aws s3api list-object-versions --no-sign-request --bucket "$bucket" > "$vfile"
    done
}
Sync() {
    local sdir="$1" vdir="$2" group="$3"; shift; shift; shift
    mkdir -p "$sdir"
    echo '*' > "$sdir/.gitignore"
    echo "Sync: $sdir $vdir $group" 1>&2
    if [ -n "${FETCHER_SKIP_SYNC:-}" ]; then return 0; fi
    local bucketfile bucket filters=() include=() i
    for bucketfile in "$vdir"/*.json; do
        bucket="${bucketfile##*/}"
        bucket="${bucket%.json}"
        filters=(--exclude '*')
        readarray -t include < <(jq -r '.Versions[].Key' "$bucketfile" | grep -E -f <(yq -r ".fetch.\"$group\".\"$bucket\".include[]" "$cfg") | grep -v '/$' | uniq)
        for i in "${include[@]}"; do filters+=(--include "$i"); done
        echo "Sync: syncing s3://$bucket to $sdir/$group/$bucket" 1>&2
        /usr/bin/time aws s3 sync --no-sign-request --only-show-errors "${filters[@]}" "s3://$bucket" "$sdir/$group/$bucket"
    done
}
Main "$@"
# shellcheck disable=SC2317
exit 1