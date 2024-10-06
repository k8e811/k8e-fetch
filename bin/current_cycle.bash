#!/bin/bash -x
year=$(date +%Y)
((prev_year=year-1))
# https://www.ncsbe.gov/candidates/terms-office for Judges
((min_year=year-8))
Main() {
	set -Eeuo pipefail
	buckets=(dl.ncsbe.gov)
	FetchVersions "${buckets[@]}"
	FilterRelevant "${buckets[@]}"
	Sync "${buckets[@]}"
	SQLoad "${buckets[@]}"
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
Main "$@"
# shellcheck disable=SC2317
exit 1
