#!/bin/bash

SNAP=$1
OUTPUT=$2
SNAP_PATH=".farmfs/keys/snaps/$SNAP"

rm -f "$SNAP.list"
cat "$SNAP_PATH" | head -1 | jq '.[]|select(.[1]=="link")|".farmfs/userdata"+.[2]' | perl -pe 's/^"(.*)"$/$1/' > "$SNAP.list"
echo "$SNAP_PATH" >> "$SNAP.list"

tar -cvf "$OUTPUT" -T "$SNAP.list"

rm "$SNAP.list"
