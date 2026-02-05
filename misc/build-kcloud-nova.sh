#!/bin/sh

rm -rf loci/data/nova/
cp -a kcloud-nova loci/data/nova

sudo docker build \
    -f loci/Dockerfile \
    --build-arg FROM=ghcr.io/openkcloud/kcloud-nova_base:ubuntu_jammy \
    --build-arg WHEELS=ghcr.io/openkcloud/kcloud-nova_requirements:stable-2024.1-ubuntu_jammy \
    --build-arg PROJECT=nova \
    --build-arg PROJECT_REPO=https://github.com/openkcloud/kcloud-nova \
    --build-arg PROJECT_REF=stable/2024.1 \
    --build-arg PROFILES='fluent ceph linuxbridge openvswitch configdrive qemu apache migration' \
    --build-arg DIST_PACKAGES='net-tools openssh-server' \
    --tag ghcr.io/openkcloud/kcloud-nova:stable-2024.1 \
    loci

sudo docker save ghcr.io/openkcloud/kcloud-nova:stable-2024.1 -o /tmp/kcloud-nova_stable-2024.1.tar
sudo chown kcloud.kcloud /tmp/kcloud-nova_stable-2024.1.tar

#
# in worker nodes
#

# sudo ctr -n k8s.io images rm ghcr.io/openkcloud/kcloud-nova:stable-2024.1
# scp kcloud@<build server>:/tmp/kcloud-nova_stable-2024.1.tar /tmp/
# sudo ctr -n k8s.io images import /tmp/kcloud-nova_stable-2024.1.tar