// Builds dashboards in Grafana's v2 dashboard schema (dashboard.grafana.app/v2), which adds layouts
// such as tabs. The variables, links and settings are converted from a dashboard built with the
// dashboard() helper, and the panels from grafana-builder rows, so the same rows can be shown both
// in a v1 dashboard and in a tab of a v2 one.

// Enum values of the v2 schema, indexed by the matching v1 number.
local sortModes = [
  'disabled',
  'alphabeticalAsc',
  'alphabeticalDesc',
  'numericalAsc',
  'numericalDesc',
  'alphabeticalCaseInsensitiveAsc',
  'alphabeticalCaseInsensitiveDesc',
  'naturalAsc',
  'naturalDesc',
];
local hideModes = ['dontHide', 'hideLabel', 'hideVariable'];
local refreshModes = ['never', 'onDashboardLoad', 'onTimeRangeChanged'];
local cursorSyncModes = ['Off', 'Crosshair', 'Tooltip'];

// Height of a grafana-builder row in grid units, as Grafana computes it when migrating legacy rows.
local rowHeight(row) =
  local px = std.parseInt(std.strReplace(std.get(row, 'height', '250px'), 'px', ''));
  std.ceil(std.max(px, 90) / 38);

// rowPanels places the panels of a grafana-builder row like Grafana's migration of legacy rows:
// each panel is span * 2 wide, starting a new line when the next one doesn't fit.
local rowPanels(row) =
  local h = rowHeight(row);
  std.foldl(
    function(acc, p)
      local w = std.get(p, 'span', 4) * 2;
      local wrap = acc.x + w > 24;
      local x = if wrap then 0 else acc.x;
      local y = if wrap then acc.y + h else acc.y;
      { panels: acc.panels + [p { gridPos: { x: x, y: y, w: w, h: h } }], x: x + w, y: y },
    row.panels,
    { panels: [], x: 0, y: 0 },
  ).panels;

local orDefault(value, default) = if value == null then default else value;

// v1 datasources are either a variable or uid string, or a { type, uid } reference.
local datasourceName(ds) = if std.isObject(ds) then ds.uid else ds;
local datasourceType(ds) = if std.isObject(ds) then std.get(ds, 'type', 'prometheus') else 'prometheus';

local dataQuery(ds, spec) = {
  kind: 'DataQuery',
  group: datasourceType(ds),
  version: 'v0',
  datasource: { name: datasourceName(ds) },
  spec: spec,
};

local current(variable) = {
  local c = std.get(variable, 'current', {}),
  text: std.get(c, 'text', ''),
  value: std.get(c, 'value', ''),
};

local variable(v) =
  local common = {
    name: v.name,
    label: std.get(v, 'label', ''),
    hide: hideModes[std.get(v, 'hide', 0)],
    skipUrlSync: std.get(v, 'skipUrlSync', false),
    current: current(v),
    options: [],
    multi: std.get(v, 'multi', false),
    includeAll: std.get(v, 'includeAll', false),
    allowCustomValue: true,
    [if std.objectHas(v, 'description') then 'description']: v.description,
    [if std.get(v, 'allValue') != null then 'allValue']: v.allValue,
  };
  if v.type == 'datasource' then {
    kind: 'DatasourceVariable',
    spec: common {
      pluginId: v.query,
      refresh: refreshModes[std.get(v, 'refresh', 1)],
      regex: std.get(v, 'regex', ''),
    },
  } else if v.type == 'query' then {
    kind: 'QueryVariable',
    spec: common {
      query: dataQuery(v.datasource, { __legacyStringValue: v.query }),
      refresh: refreshModes[std.get(v, 'refresh', 1)],
      regex: std.get(v, 'regex', ''),
      sort: sortModes[std.get(v, 'sort', 0)],
    },
  } else if v.type == 'custom' then {
    kind: 'CustomVariable',
    spec: common {
      query: v.query,
    },
  } else error 'dashboard-v2: unsupported variable type %s' % v.type;

local annotation(a) = {
  kind: 'AnnotationQuery',
  spec: {
    name: a.name,
    enable: std.get(a, 'enable', true),
    hide: std.get(a, 'hide', false),
    iconColor: std.get(a, 'iconColor', 'red'),
    query: dataQuery(a.datasource, {
      [k]: a[k]
      for k in std.objectFields(a)
      if !std.member(['name', 'enable', 'hide', 'iconColor', 'datasource'], k)
    }),
  },
};

// The "Annotations & Alerts" annotation, which v1 dashboards get implicitly.
local builtInAnnotation = {
  kind: 'AnnotationQuery',
  spec: {
    name: 'Annotations & Alerts',
    builtIn: true,
    enable: true,
    hide: true,
    iconColor: 'rgba(0, 211, 255, 1)',
    query: {
      kind: 'DataQuery',
      group: 'grafana',
      version: 'v0',
      datasource: { name: '-- Grafana --' },
      spec: {},
    },
  },
};

local link(l) = {
  title: std.get(l, 'title', ''),
  type: l.type,
  icon: std.get(l, 'icon', ''),
  tooltip: std.get(l, 'tooltip', ''),
  url: std.get(l, 'url', ''),
  tags: std.get(l, 'tags', []),
  asDropdown: std.get(l, 'asDropdown', false),
  targetBlank: std.get(l, 'targetBlank', false),
  includeVars: std.get(l, 'includeVars', false),
  keepTime: std.get(l, 'keepTime', false),
};

// v2 types noValue as a string, while v1 accepts any value.
local fieldConfig(fc) = fc + (
  if std.objectHas(std.get(fc, 'defaults', {}), 'noValue')
  then { defaults+: { noValue: std.toString(fc.defaults.noValue) } }
  else {}
);

local queryOptionNames = ['interval', 'maxDataPoints', 'timeFrom', 'timeShift', 'hideTimeOverride', 'cacheTimeout', 'queryCachingTTL'];

local panel(p, id) =
  local ds = std.get(p, 'datasource');
  local targets = std.get(p, 'targets', []);
  {
    kind: 'Panel',
    spec: {
      id: id,
      title: std.get(p, 'title', ''),
      description: std.get(p, 'description', ''),
      links: std.get(p, 'links', []),
      [if std.get(p, 'transparent', false) then 'transparent']: true,
      data: {
        kind: 'QueryGroup',
        spec: {
          queries: [
            local t = targets[i];
            {
              kind: 'PanelQuery',
              spec: {
                refId: std.get(t, 'refId', std.char(65 + i)),
                hidden: std.get(t, 'hide', false),
                query: dataQuery(std.get(t, 'datasource', ds), {
                  [k]: t[k]
                  for k in std.objectFields(t)
                  if !std.member(['refId', 'hide', 'datasource'], k) && t[k] != null
                }),
              },
            }
            for i in std.range(0, std.length(targets) - 1)
          ],
          transformations: [{ kind: t.id, spec: t } for t in std.get(p, 'transformations', [])],
          queryOptions: { [k]: p[k] for k in queryOptionNames if std.get(p, k) != null },
        },
      },
      vizConfig: {
        kind: 'VizConfig',
        group: p.type,
        version: std.get(p, 'pluginVersion', ''),
        spec: {
          options: orDefault(std.get(p, 'options'), {}),
          fieldConfig: fieldConfig(orDefault(std.get(p, 'fieldConfig'), { defaults: {}, overrides: [] })),
        },
      },
    },
  };

{
  // tabbedDashboard returns dashboard as a v2 dashboard with a tab for each { key, title, rows }
  // item of tabs, where rows are grafana-builder rows. With a single tab, the rows are shown without tabs.
  tabbedDashboard(dashboard, tabs)::
    // Place the rows of each tab, collecting their panels as elements named after the tab.
    // Panel ids are unique across the dashboard.
    local placed = std.foldl(
      function(acc, tab)
        local rows = std.foldl(
          function(rowAcc, row)
            // Skip the empty placeholders some helpers return, for example the ephemeral storage
            // panel outside of Kubernetes, keeping their space on the grid.
            local panels = [p for p in rowPanels(row) if std.objectHas(p, 'type')];
            local names = std.makeArray(std.length(panels), function(i) '%s-panel-%d' % [tab.key, rowAcc.nextId + i]);
            {
              nextId: rowAcc.nextId + std.length(panels),
              elements: rowAcc.elements {
                [names[i]]: panel(panels[i], rowAcc.nextId + i)
                for i in std.range(0, std.length(panels) - 1)
              },
              rows: rowAcc.rows + [{
                kind: 'RowsLayoutRow',
                spec: {
                  title: row.title,
                  collapse: std.get(row, 'collapse', false),
                  [if !std.get(row, 'showTitle', true) then 'hideHeader']: true,
                  layout: {
                    kind: 'GridLayout',
                    spec: {
                      items: [
                        {
                          kind: 'GridLayoutItem',
                          spec: {
                            x: panels[i].gridPos.x,
                            y: panels[i].gridPos.y,
                            width: panels[i].gridPos.w,
                            height: panels[i].gridPos.h,
                            element: { kind: 'ElementReference', name: names[i] },
                          },
                        }
                        for i in std.range(0, std.length(panels) - 1)
                      ],
                    },
                  },
                },
              }],
            },
          tab.rows,
          { nextId: acc.nextId, elements: acc.elements, rows: [] },
        );
        {
          nextId: rows.nextId,
          elements: rows.elements,
          tabs: acc.tabs + [{ title: tab.title, layout: { kind: 'RowsLayout', spec: { rows: rows.rows } } }],
        },
      tabs,
      { nextId: 1, elements: {}, tabs: [] },
    );

    {
      // The same panels and variables in a v1 dashboard, for tools that only support v1 such as
      // mixtool lint, with the rows of every tab one after the other.
      v1:: dashboard.addRows(std.flattenArrays([tab.rows for tab in tabs])),

      apiVersion: 'dashboard.grafana.app/v2',
      kind: 'Dashboard',
      metadata: { name: dashboard.uid },
      spec: {
        title: dashboard.title,
        description: std.get(dashboard, 'description', ''),
        tags: dashboard.tags,
        editable: dashboard.editable,
        cursorSync: cursorSyncModes[dashboard.graphTooltip],
        liveNow: false,
        preload: false,
        links: [link(l) for l in dashboard.links],
        timeSettings: {
          timezone: dashboard.timezone,
          from: dashboard.time.from,
          to: dashboard.time.to,
          autoRefresh: dashboard.refresh,
          autoRefreshIntervals: dashboard.timepicker.refresh_intervals,
          hideTimepicker: false,
          fiscalYearStartMonth: 0,
        },
        annotations: [builtInAnnotation] + [annotation(a) for a in dashboard.annotations.list],
        variables: [variable(v) for v in dashboard.templating.list],
        elements: placed.elements,
        layout:
          if std.length(placed.tabs) == 1
          then placed.tabs[0].layout
          else {
            kind: 'TabsLayout',
            spec: {
              tabs: [{ kind: 'TabsLayoutTab', spec: tab } for tab in placed.tabs],
            },
          },
      },
    },
}
