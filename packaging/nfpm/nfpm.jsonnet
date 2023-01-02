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
        packager: 'deb',
      },
      {
        src: './dist/tmp/dependencies-%s-%s-%s/mimir.env' % [name, packager, arch],
        dst: '/etc/sysconfig/mimir',
        packager: 'rpm',
      },
      {
        src: './docs/configurations/single-process-config-blocks.yaml',
        dst: '/etc/mimir/config.example.yaml',
        type: 'config|noreplace',
      },
    ],
    scripts: {
      postinstall: './dist/tmp/dependencies-%s-%s-%s/postinstall.sh' % [name, packager, arch],
      preremove: './dist/tmp/dependencies-%s-%s-%s/preremove.sh' % [name, packager, arch],
    },
  },
};

{
  name: name,
  arch: arch,
  platform: 'linux',
  version: '${VERSION}',
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
