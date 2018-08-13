#!/bin/bash

cat $1 | python mapred_rider_level.py m | sort | \
  python mapred_rider_level.py r "$2" "$3" > output_rider_level
