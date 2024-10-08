#!/bin/bash -x
year=$(date +%Y)
((prev_year=year-1))
# https://www.ncsbe.gov/candidates/terms-office for Judges
((min_year=year-8))
zero=$(readlink -f "$0")
zero_dir="${zero%/*}"
Main() {
	#shellcheck disable=SC2034
	OPATH="$PATH"
	PATH="$zero_dir:$PATH"
	set -Eeuo pipefail
	buckets=(dl.ncsbe.gov)
	FetchVersions "${buckets[@]}"
	FilterRelevant "${buckets[@]}"
	Sync "${buckets[@]}"
	SQLLoad "${buckets[@]}"
	exit 0
}
FetchVersions() {
	local bucket
	for bucket in "$@"; do
		aws s3api list-object-versions --no-sign-request --bucket "$bucket" > "$bucket.versions.json"
	done
}
FilterRelevant() {
	local bucket filter
	filter=$(mktemp)
	MkFilter > "$filter"
	for bucket in "$@"; do
		jq -r '.Versions[].Key' "$bucket.versions.json" |
			grep -E -f "$filter" |grep -v '/$' | uniq > "$bucket.fetch"
	done
	rm -vf "$filter"
}
MkFilter() {
	local y
	echo '_Statewide.zip$'
	for y in "$prev_year" "$year"; do
		echo "Snapshots/VR_Snapshot_$y"
	done
	for y in $(seq "$min_year" "$year"); do
		echo "Elections/$y"
	done
}
Sync() {
	local bucket i include=() filters=()
	for bucket in "$@"; do
		mkdir -p "$bucket"
		readarray -t include < "$bucket.fetch"
		filters=(--exclude '*')
		for i in "${include[@]}"; do
			filters+=(--include "$i")
		done
		aws s3 sync --no-sign-request --only-show-errors "${filters[@]}" "s3://$bucket" "fetch/$bucket"
	done
}
SQLLoad() {
	local bucket i cmds=('.echo on') item_cmds=() init=''
	init=$(mktemp)
	for bucket in "$@"; do
		Flatten "$bucket"
		for i in "flat/$bucket"/*; do
			readarray -t item_cmds < <(ItemCmds "$i")
			cmds+=("${item_cmds[@]}")
		done
	done
	for i in "${cmds[@]}"; do echo "$i"; done > "$init"
	rm -f 1.sqlite || :
	sqlite3 1.sqlite -init "$init" ".exit"
}
Flatten() {
	local bucket item items=()
	for bucket in "$@"; do
		mkdir -p "flat/$bucket"
		readarray -t items < "$bucket.fetch"
		for item in "${items[@]}"; do
			ln --symbolic --no-dereference --force --relative "fetch/$bucket/$item" "flat/$bucket/"
		done
	done
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
