#!/bin/bash

echo "Building kernel headers"
apt-get install -y linux-headers-$(uname -r)

mount -t debugfs none /sys/kernel/debug

#echo "Starting fileaccess.py..."

#python3 fileaccess.py $@


python3 cgroup_informer.py $@