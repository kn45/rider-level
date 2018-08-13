#!/bin/bash

cat $1 | python mapred_rider_level.py m | sort | \
  python mapred_rider_level.py r "$2" "$3" > output_rider_level

cat output_rider_level | sort | python mapred_month_level.py r "$2" "$3" > output_month_level

cat output_rider_level | sort | python mapred_level_speed.py r "$2" "$3" > output_level_speed
