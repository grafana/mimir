---
description: Learn how to deploy Grafana Mimir with puppet.
keywords:
  - Mimir deployment
  - Puppet
menuTitle: Deploy with Puppet
title: Deploy Grafana Mimir with Puppet
weight: 13
---

# Deploy Grafana Mimir with Puppet

A puppet module is community maintained to deploy Grafana Mimir. This module use the deb or rpm packages to install Grafana Mimir.

## Start using this module

Add this module to your Puppetfile:

```
mod 'ovhcloud-mimir', '1.0.0'
```

## Install and configure Mimir

Call the main class of the module inside a puppet manifest like this:

```
class{'mimir': }
```

If called like this, this module will use the default configuration parameters of Mimir and therefore start mimir in standalone mode.

To customize the configuration, just add a hash with the appropriate configuration key like this:

```
class {'mimir':
    config_hash => {
        target => 'querier',
    },
}
```

## Configuration parameters

```yaml
# Mimir version under the form X.X.X or one of the supported puppet values
# as 'present', 'latest', ...
[package_ensure: <string> | default = "present"]

# Boolean to specify if module should manage mimir user
[manage_user: <boolean> | default = false]

# Home directory for the managed user
# Only if manage_user is set to true
[user_home: <string> | default = "/var/lib/mimir"]

# Binary to use as shell for managed user
# Only if manage_user is set to true
[user_shell: <string> | default = "/sbin/nologin"]

# Additionnal groups the managed user should be connected to
# Only if manage_user is set to true
[user_extra_groups: <array> | default =  []]

# Directory to store the mimir configuration
[config_dir: <string> | default = "/etc/mimir"]

# Group to use for configuration resources
[config_group: <string> | default = "mimir"]

# Hash containing the configuration keys to override
[config_hash: <hash> | default = {}]

# Owner to use for configuration resources
[config_owner: <string> | default = "mimir"]

# Additional arguments to set to the mimir process
[custom_args: <array> | default = []]

# Directory to store mimir logs if log to file is enabled
# Only if log_to_file is set to true
[log_dir_path: <string> | default = "/var/log/mimir"]

# Mode of the directory used to store logs
# Only if log_to_file is set to true
[log_dir_mode: <string> | default = "0700"]

# Filename to store mimir logs if log to file is enabled
# Only if log_to_file is set to true
[log_file_path: <string> | default = "mimir.log"]

# Mode of the file used to store logs
# Only if log_to_file is set to true
[log_file_mode: <string> | default = "0600"]

# Group to use for log resources
# Only if log_to_file is set to true
[log_group: <string> | default = "root"]

# Log level to use for process mimir
[log_level: <string> | default = "info"]

# Owner to use for log resources
# Only if log_to_file is set to true
[log_owner: <string> | default = "root"]

# Should log be kept in journald or sent to a dedicated file
[log_to_file: <boolean> | default = false]

# Command use to validate configuration
[validate_cmd: <string> | default = "/usr/local/bin/mimir --modules=true"]

# Command use to restart/reload process
[restart_cmd: <string> | default = "/bin/systemctl reload mimir"]

# Should the process be restarted on configuration changes
[restart_on_change: <boolean> | default = false]

# List of systemd parameters to override
[systemd_overrides: <hash> | default = {'Service' => {'LimitNOFILE' => '1048576'}}]
```

## References

- [Full module documentation](https://forge.puppet.com/modules/ovhcloud/mimir/readme)
- [Module source code](https://github.com/ovh/puppet-mimir)
