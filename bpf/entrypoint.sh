#!/bin/bash

mount -t debugfs none /sys/kernel/debug

echo "Starting fileaccess.py..."

python3 fileaccess.py $@