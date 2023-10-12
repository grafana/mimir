local name = std.extVar('name');
local arch = std.extVar('arch');
local packager = std.extVar('packager');

local overrides = {
  mimir: {
    description: |||
      Grafana Mimir provides horizontally scalable, highly available, multi-tenant, long-term storage for Prometheus.
    |||,
    contents+: [
      {
        src: './dist/tmp/dependencies-%s-%s-%s/mimir.service' % [name, packager, arch],
        dst: '/lib/systemd/system/mimir.service',
        packager: 'deb',
      },
      {
        src: './dist/tmp/dependencies-%s-%s-%s/mimir.service' % [name, packager, arch],
        dst: '/lib/systemd/system/mimir.service',
        packager: 'rpm',
      },
      {
        src: './dist/tmp/dependencies-%s-%s-%s/mimir.env' % [name, packager, arch],
        dst: '/etc/default/mimir',
        type: 'config|noreplace',
        packager: 'deb',
      },
      {
        src: './dist/tmp/dependencies-%s-%s-%s/mimir.env' % [name, packager, arch],
        dst: '/etc/sysconfig/mimir',
        type: 'config|noreplace',
        packager: 'rpm',
      },
      {
        src: './docs/configurations/single-process-config-blocks.yaml',
        dst: '/etc/mimir/config.example.yaml',
        type: 'config|noreplace',
      },
      {
        src: './dist/tmp/dependencies-%s-%s-%s/config.yml' % [name, packager, arch],
        dst: '/etc/mimir/config.yml',
        type: 'config|noreplace',
      },
      {
        src: './dist/tmp/dependencies-%s-%s-%s/runtime_config.yml' % [name, packager, arch],
        dst: '/etc/mimir/runtime_config.yml',
        type: 'config|noreplace',
      },
      {
        src: './dist/tmp/dependencies-%s-%s-%s/mimir.logrotate' % [name, packager, arch],
        dst: '/etc/logrotate.d/mimir',
        type: 'config|noreplace',
      },
    ],
    scripts: {
      postinstall: './dist/tmp/dependencies-%s-%s-%s/postinstall.sh' % [name, packager, arch],
      preremove: './dist/tmp/dependencies-%s-%s-%s/preremove.sh' % [name, packager, arch],
    },
  },
  metaconvert: {
    description: |||
      Grafana Metaconvert converts Cortex meta.json files to be on the Grafana Mimir format.
    |||,
  },
  'mimir-continuous-test': {
    description: |||
      As a developer, you can use the standalone mimir-continuous-test tool to run smoke tests on live Grafana Mimir clusters. This tool identifies a class of bugs that could be difficult to spot during development.
    |||,
  },
  mimirtool: {
    description: |||
      Mimirtool is a command-line tool that operators and tenants can use to execute a number of common tasks that involve Grafana Mimir or Grafana Cloud Metrics.
    |||,
  },
  'query-tee': {
    description: |||
      The query-tee is a standalone tool that you can use for testing purposes when comparing the query results and performances of two Grafana Mimir clusters. The two Mimir clusters compared by the query-tee must ingest the same series and samples.
    |||,
  },
};

{
  name: name,
  arch: arch,
  platform: 'linux',
  version: '${VERSION}',
  version_schema: 'none',  // Don't parse our version, as nfpm decides to issue x.y.z-rc.0 as x.y.z~rc.0 and we have to mangle the version. Just trust the version we want to issue.
  section: 'default',
  provides: [name],
  maintainer: 'Grafana Labs <contact@grafana.com>',
  vendor: 'Grafana Labs',
  homepage: 'https://grafana.com/oss/mimir/',
  license: 'AGPL-3.0',
  contents: [{
    src: './dist/tmp/packages/%s-linux-%s' % [name, arch],
    dst: '/usr/local/bin/%s' % name,
  }],
} + overrides[name]
