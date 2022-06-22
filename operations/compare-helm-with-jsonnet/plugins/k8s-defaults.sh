#!/bin/bash

# Here we use kubectl's server-side dry run feature to apply all k8s default values
# This process sets things like default storage class, default image pull policy, etc.
# It also adds a few other fields that are different on every invocation like metadata.uid
# We remove the latter and format the output to make it compatible with kustomize

yq '.items[] | split_doc' |
    kubectl --dry-run=server apply -f - -o yaml |
    yq 'del(.metadata) | .kind = "ResourceList" | .apiVersion = "config.kubernetes.io/v1"' |
    yq '.items = ([.items[] | del(.metadata.annotations["kubectl.kubernetes.io/last-applied-configuration"])])' |
    yq '.items = ([.items[] | del(.metadata.creationTimestamp)])' |
    yq '.items = ([.items[] | del(.metadata.managedFields)])' |
    yq '.items = ([.items[] | del(.metadata.uid)])'
