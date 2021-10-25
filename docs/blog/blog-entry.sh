#!/bin/bash

date_timestamp=$(date +%s.%y-%m-%d)
echo "mkdir -p ./logs/$date_timestamp/"
echo "touch ./logs/$date_timestamp/note.text"
echo "touch ./logs/$date_timestamp/"
