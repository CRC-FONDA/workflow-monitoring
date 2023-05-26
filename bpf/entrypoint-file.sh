#!/bin/bash

echo "Building kernel headers"
apt-get install -y linux-headers-$(uname -r)

mount -t debugfs none /sys/kernel/debug

echo "Starting fileaccess.py..."
echo "Your container args are: $@"

python3 fileaccess.py $@