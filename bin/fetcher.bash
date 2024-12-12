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
FETCHER_SKIP_COMPRESS=true
Main() {
    : "${OPATH:=$PATH}"
    PATH="$zerodir:$PATH"
    local group="${1:-current_cycle}"
    local buckets=()
    readarray -t buckets < <(yq -r ".fetch.$group | keys().[]" < "$cfg")
    time FetchVersions "${buckets[@]}"
    time Sync "$group"
    time SqlLoad "$group"
    time Compress "$group"
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
    local mode=tabs encoding='LATIN1' base qq t1 table zip email
	base="${1##*/}"
	t1="${base%%.*}"
	table="${t1,,}"
	qq="header_fix"
	zip="unzip -p '$1'"
    email=''
	case "$base" in
		Candidate_Listing_*.csv)
			zip="cat '$1'"
			mode=csv
			qq="quotequote"
            email="ALTER TABLE $table ADD COLUMN \"email\" TEXT;"
			;;
		VR_Snapshot_*.zip)
			qq="quotequote"
			case "${base##*_}" in
				2005*|2006*|2007*) : ;;
                *) encoding=UTF-16 ;;
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
    if [ -n "$email" ]; then echo "$email"; fi
}

Compress() {
    if [ -n "${FETCHER_SKIP_COMPRESS:-}" ]; then return 0; fi
    local group="$1"; 
    local db="$dbdir/$group.load.sqlite"
    local tables="$dbdir/$group.load.tables"
    echo "Compress: $group" 1>&2
    sqlite3 "$db" "SELECT name FROM sqlite_schema WHERE type='table' and name NOT LIKE 'sqlite_%';" > "$tables"
    local compressors=() c
    readarray -t compressors < <(yq -r ".compress | keys().[]" < "$cfg")
    rm -f "$tables".* || :
    for c in "${compressors[@]}"; do
        grep -E -f <(yq -r ".compress.$c.include[]" "$cfg") "$tables" > "$tables.$c"
        CompressComponents "$db" "$c" "$tables"
    done
}
CompressComponents() {
    local db="$1"; shift
    local compressor="$1"; shift
    local tables="$1"; shift
    echo "CompressComponents: $db, $compressor, $tables" 1>&2
    local components=() component columns=() c_tables=() table q_columns s_columns=() qs_columns
    readarray -t c_tables < "$tables.$compressor"
    set -x
    readarray -t components < <(yq -r ".compress.$compressor.components | keys().[]" "$cfg")
    echo "CompressComponents: ${components[*]}" 1>&2
    for component in "${components[@]}"; do
        readarray -t columns < <(yq -r ".components.$component.[]" "$cfg")
        readarray -t s_columns < <(SourceColumns "$compressor" "$component" "${columns[@]}")
        echo "CompressComponents: $component, ${columns[*]}" 1>&2
        readarray -t c_tables < "$tables.$compressor"
        IFS=, q_columns="${columns[*]}"
        IFS=, qs_columns="${s_columns[*]}"
        for table in "${c_tables[@]}"; do
            sqlite3 "$db" -batch -echo "CREATE TABLE IF NOT EXISTS $component AS SELECT $qs_columns FROM $table LIMIT 0;"
            sqlite3 "$db" -batch -echo "CREATE UNIQUE INDEX IF NOT EXISTS u_$component ON $component($q_columns);"
            sqlite3 "$db" -batch -echo "INSERT OR IGNORE INTO $component SELECT DISTINCT $qs_columns FROM $table;"
        done
    done
}
SourceColumns() {
    local compressor="$1"; shift
    local component="$1"; shift
    for c in "$@"; do
        echo "$(yq -r ".compress.$compressor.components.$component.subst.$c // \"$c\"" "$cfg") as $c"
    done
}
Main "$@"
# shellcheck disable=SC2317
exit 1
