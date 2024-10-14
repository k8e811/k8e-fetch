#!/bin/bash
set -Eeuo pipefail
zero=$(readlink -f "$0")
zerodir="${zero%/*}"
cfg="${FETCHER_CONFIG:-${zerodir}/cfg.yaml}"
opwd="$PWD"
datadir="${FETCHER_DATADIR:-${opwd}}"
versionsdir="${FETCHER_VERSIONSDIR:-${datadir}/versions}"
syncdir="${FETCHER_SYNCDIR:-${datadir}/sync}"
flatdir="${FETCHER_FLATDIR:-${datadir}/flat}"
dbdir="${FETCHER_DBDIR:-${datadir}/db}"
Main() {
    : "${OPATH:=$PATH}"
    PATH="$zerodir:$PATH"
    local group="${1:-current_cycle}"
    local buckets=()
    readarray -t buckets < <(yq -r ".fetch.$group | keys().[]" < "$cfg")
    FetchVersions "${buckets[@]}"
    Sync "$group"
    SqlLoad "$group"
    exit 0
}
FetchVersions() {
    local vfile bucket vdir="$versionsdir"
    mkdir -pv "$vdir"
    echo '*' > "$vdir/.gitignore"
    echo "FetchVersions: $vdir," "$@" 1>&2
    if [ -n "${FETCHER_SKIP_VERSIONS:-}" ]; then return 0; fi
    for bucket in "$@"; do
        vfile="$vdir/$bucket.json"
        aws s3api list-object-versions --no-sign-request --bucket "$bucket" > "$vfile"
    done
}
Sync() {
    local group="$1"; shift
    local sdir="$syncdir" vdir="$versionsdir" 
    mkdir -p "$sdir"
    echo '*' > "$sdir/.gitignore"
    echo "Sync: $sdir $vdir $group" 1>&2
    if [ -n "${FETCHER_SKIP_SYNC:-}" ]; then return 0; fi
    local bucketfile bucket filters=() include=() i
    for bucketfile in "$vdir"/*.json; do
        bucket="${bucketfile##*/}"
        bucket="${bucket%.json}"
        filters=(--exclude '*')
        readarray -t include < <(Include "$bucketfile" "$group" "$bucket" "$cfg")
        for i in "${include[@]}"; do filters+=(--include "$i"); done
        echo "Sync: syncing s3://$bucket to $sdir/$group/$bucket" 1>&2
        /usr/bin/time aws s3 sync --no-sign-request --only-show-errors "${filters[@]}" "s3://$bucket" "$sdir/$group/$bucket"
    done
}
Include() {
    local bucketfile="$1" group="$2" bucket="$3" cfg="$4"
    jq -r '.Versions[].Key' "$bucketfile" | grep -E -f <(yq -r ".fetch.\"$group\".\"$bucket\".include[]" "$cfg") | grep -v '/$' | uniq
}
SqlLoad() {
    local group="$1"; shift
    local sdir="$syncdir" vdir="$versionsdir" fdir="$flatdir"
    local bucketfile bucket include=() i fbdir sbdir
    echo "SqlLoad: $sdir $vdir $group $fdir" 1>&2
    mkdir -p "$fdir"
    echo "*" > "$fdir/.gitignore"
    mkdir -p "$dbdir"
    echo "*" > "$dbdir/.gitignore"
    if [ -n "${FETCHER_SKIP_LOAD:-}" ]; then return 0; fi
    local cmds=(".echo on") item_cmds=()
    for bucketfile in "$vdir"/*.json; do
        bucket="${bucketfile##*/}"
        bucket="${bucket%.json}"
        readarray -t include < <(Include "$bucketfile" "$group" "$bucket" "$cfg")
        fbdir="$fdir/$group/$bucket"
        sbdir="$sdir/$group/$bucket"
        echo "SqlLoad: flattening $sbdir to $fbdir" 1>&2
        rm -rf "$fbdir" || :
        mkdir -p "$fbdir"
        (cd "$sbdir" || exit 1; ln --symbolic --no-dereference --force --relative --target-directory="$fbdir/" "${include[@]}")
        for i in "$fbdir"/*; do
            readarray -t item_cmds < <(ItemCmds "$i")
            cmds+=("${item_cmds[@]}")
        done
    done
    local dbload="$dbdir/$group.load.sql"
    for i in "${cmds[@]}"; do echo "$i"; done > "$dbload"
    local db="$dbdir/$group.load.sqlite"
    rm -f "$db" || :
    sqlite3 "$db" -init "$dbload" ".exit"
}
ItemCmds() {
    local mode=tabs encoding='LATIN1' base qq t1 table zip
	base="${1##*/}"
	t1="${base%%.*}"
	table="${t1,,}"
	qq="header_fix"
	zip="unzip -p '$1'"
	case "$base" in
		Candidate_Listing_*.csv)
			zip="cat '$1'"
			mode=csv
			qq="quotequote"
			;;
		VR_Snapshot_*.zip)
			encoding=UTF-16
			qq="quotequote"
			case "${base##*_}" in
				2005*|2006*|2007*) encoding=LATIN1 ;;
			esac
			;;
		ncvoter_Statewide.zip|ncvhis_Statewide.zip)
			:
			;;
		*)
			return
			;;
	esac
	echo ".mode $mode"
	echo ".import \"| $zip | iconv -f $encoding -t UTF-8 - | $qq\" $table"
}
Main "$@"
# shellcheck disable=SC2317
exit 1