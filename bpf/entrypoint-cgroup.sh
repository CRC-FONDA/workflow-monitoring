#!/bin/bash

echo "Building kernel headers"
apt update
apt-get install -y linux-headers-$(uname -r)

mount -t debugfs none /sys/kernel/debug

python3 cgroup_informer.py $@
