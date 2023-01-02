local overrides = {
  mimir: {
    description: |||
      Grafana Mimir provides horizontally scalable, highly available, multi-tenant, long-term storage for Prometheus.
    |||,
    contents+: [
      {
        src: './packaging/deb/systemd/mimir.service',
        dst: '/lib/systemd/system/mimir.service',
        packager: 'deb',
        file_info: {
          owner: 0,
          group: 0,
        },
      },
      {
        src: './packaging/rpm/systemd/mimir.service',
        dst: '/lib/systemd/system/mimir.service',
        packager: 'rpm',
        file_info: {
          owner: 0,
          group: 0,
        },
      },
      {
        src: './packaging/deb/default/mimir',
        dst: '/etc/default/mimir',
        packager: 'deb',
        file_info: {
          owner: 0,
          group: 0,
        },
      },
      {
        src: './packaging/rpm/sysconfig/mimir',
        dst: '/etc/sysconfig/mimir',
        packager: 'rpm',
        file_info: {
          owner: 0,
          group: 0,
        },
      },
      {
        src: './docs/configurations/single-process-config-blocks.yaml',
        dst: '/etc/mimir/config.example.yaml',
        type: 'config|noreplace',
        file_info: {
          owner: 0,
          group: 0,
        },
      },
    ],
    overrides: {
      deb: {
        scripts: {
          postinstall: './packaging/deb/control/postinst',
          preremove: './packaging/deb/control/prerm'
        },
      },
      rpm: {
        scripts: {
          postinstall: './packaging/rpm/control/post',
          preremove: './packaging/rpm/control/preun'
        },
      },
    },
  },
};

local name = std.extVar('name');
local arch = std.extVar('arch');

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
