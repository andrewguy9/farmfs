#!/bin/bash

SNAP=$1
OUTPUT=$2
SNAP_PATH=".farmfs/keys/snaps/$SNAP"

rm -f "$SNAP.list"
cat "$SNAP_PATH" | head -1 | jq -r '.[]|select(.type=="link")|.csum|capture("^(?<a>.{3})(?<b>.{3})(?<c>.{3})(?<d>.*)$")|".farmfs/userdata/"+.a+"/"+.b+"/"+.c+"/"+.d' > "$SNAP.list"
echo "$SNAP_PATH" >> "$SNAP.list"

tar -cvf "$OUTPUT" -T "$SNAP.list"

rm "$SNAP.list"
