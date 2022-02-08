---
title: "About configuration parameter categories"
description: ""
weight: 10
---

# About configuration parameter categories

In order to simplify Mimir configuration, parameters are bucketed into 3 categories according to
their maturity and intended use: _basic_, _advanced_, and _experimental_.

## Basic

Parameters that we expect the majority of users to modify. For example, object store credentials or
other dependency connection information. These parameters will generally remain stable for long periods
of time, and should focus on user goals.

## Advanced

Parameters that we expect the majority of users to leave defaulted, but we have evidence that there
are strong use-cases to change the default value.

## Experimental

Parameters related to new and experimental features under testing. These parameters are provided
for users who wish to become early adopters and for Mimir developers to gain confidence with new
features.
