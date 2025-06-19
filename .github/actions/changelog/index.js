import {appendFileSync, writeFileSync} from 'fs';
import {exec as execCallback} from 'node:child_process';
import {promisify} from 'node:util';
import {findPreviousVersion, semverParse} from "./semver.js";

//
// Github Action core utils: logging (notice + debug log levels), must escape
// newlines and percent signs
//
const escapeData = (s) => s.replace(/%/g, '%25').replace(/\r/g, '%0D').replace(/\n/g, '%0A');
const LOG = (msg) => console.log(`::notice::${escapeData(msg)}`);


// Using `git tag -l` output find the tag (version) that goes semantically
// right before the given version. This might not work correctly with some
// pre-release versions, which is why it's possible to pass previous version
// into this action explicitly to avoid this step.
const getPreviousVersion = async (version) => {
  const exec = promisify(execCallback);
  const {stdout} = await exec('git for-each-ref --sort=-creatordate --format \'%(refname:short)\' refs/tags');

  const parsedTags = stdout
    .split('\n')
    .map(semverParse)
    .filter(Boolean);

  const parsedVersion = semverParse(version);
  const prev = findPreviousVersion(parsedTags, parsedVersion);
  if (!prev) {
    throw `Could not find previous git tag for ${version}`;
  }
  return prev[5];
};


// A helper for Github GraphQL API endpoint
const graphql = async (ghtoken, query, variables) => {
  const {env} = process;
  const results = await fetch('https://api.github.com/graphql', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${ghtoken}`,
    },
    body: JSON.stringify({query, variables}),
  });

  const res = await results.json();

  LOG(
    JSON.stringify({
      status: results.status,
      text: results.statusText,
    })
  );

  return res.data;
};

// Using Github GraphQL API get a list of PRs between the two "commitish" items.
// This resoves the "since" item's timestamp first and iterates over all PRs
// till "target" using naïve pagination.
const getHistory = async (name, owner, from, to) => {
  LOG(`Fetching ${owner}/${name} PRs between ${from} and ${to}`);
  const query = `
  query findCommitsWithAssociatedPullRequests(
    $name: String!
    $owner: String!
    $from: String!
    $to: String!
    $cursor: String
  ) {
    repository(name: $name, owner: $owner) {
      ref(qualifiedName: $from) {
        compare(headRef: $to) {
          commits(first: 25, after: $cursor) {
            totalCount
            pageInfo {
              hasNextPage
              endCursor
            }
            nodes {
              id
              associatedPullRequests(first: 1) {
                nodes {
                  title
                  number
                  labels(first: 10) {
                    nodes {
                      name
                    }
                  }
                  commits(first: 1) {
                    nodes {
                      commit {
                        author {
                          user {
                            login
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }`;

  let cursor;
  let nodes = [];
  for (; ;) {
    const result = await graphql(ghtoken, query, {
      name,
      owner,
      from,
      to,
      cursor,
    });
    LOG(`GraphQL: ${JSON.stringify(result)}`);
    nodes = [...nodes, ...result.repository.ref.compare.commits.nodes];
    const {hasNextPage, endCursor} = result.repository.ref.compare.commits.pageInfo;
    if (!hasNextPage) {
      break;
    }
    cursor = endCursor;
  }
  return nodes;
};

const hasLabel = ({labels}, label) => labels.nodes.some(({name}) => name === label);

// The main function for this action: given two "commitish" items it gets a
// list of PRs between them and filters/groups the PRs by category (bugfix,
// feature, deprecation, breaking change and plugin fixes/enhancements).
//
// PR grouping relies on Github labels only, not on the PR contents.
const getChangeLogItems = async (name, owner, from, to) => {
  // get all the PRs between the two "commitish" items
  const history = await getHistory(name, owner, from, to);

  const items = history.flatMap((node) => {
    // discard PRs without a "changelog" label
    const changes = node.associatedPullRequests.nodes.filter((PR) => hasLabel(PR, 'add-to-changelog'));
    if (changes.length === 0) {
      return [];
    }
    const item = changes[0];
    const {number, url, labels} = item;
    const title = item.title.replace(/^\[[^\]]+\]:?\s*/, '');
    const author = item.commits.nodes[0].commit.author.user?.login;
    return {
      repo: name,
      number,
      title,
      author,
      labels,
    };
  });
  return items;
};

// ======================================================
//                 GENERATE CHANGELOG
// ======================================================

LOG(`Changelog action started`);
console.log(process.argv);
const ghtoken = process.env.GITHUB_TOKEN || process.env.INPUT_GITHUB_TOKEN;
if (!ghtoken) {
  throw 'GITHUB_TOKEN is not set and "github_token" input is empty';
}

const target = process.argv[2] || process.env.INPUT_TARGET;
LOG(`Target tag/branch/commit: ${target}`);

const isMajorVersion = target.endsWith('.0');

const previous = process.argv[3] || process.env.INPUT_PREVIOUS || (await getPreviousVersion(target));

LOG(`Previous tag/commit: ${previous}`);

// Get all changelog items
const oss = await getChangeLogItems('mimir', 'grafana', previous, target);

LOG(`Found OSS PRs: ${oss.length}`);

// Get the scope of the PR based on its labels
const getScope = (labels) => {
  if (hasLabel({labels}, 'scope/change')) {
    return 'CHANGE';
  } else if (hasLabel({labels}, 'scope/feature')) {
    return 'FEATURE';
  } else if (hasLabel({labels}, 'scope/enhancement')) {
    return 'ENHANCEMENT';
  } else if (hasLabel({labels}, 'scope/bugfix')) {
    return 'BUGFIX';
  }
  return 'UNKNOWN';
};

// Get the ordering of the PR based on its labels
const getScopeOrder = (labels) => {
  const scope = getScope(labels);
  switch (scope) {
    case 'CHANGE':
      return 0;
    case 'FEATURE':
      return 1;
    case 'ENHANCEMENT':
      return 2;
    case 'BUGFIX':
      return 3;
    default:
      return 4;
  }
};

// Sort PRs and categorise them into sections
const changelog = oss
  .sort((a, b) => {
    const scopeOrderA = getScopeOrder(a.labels);
    const scopeOrderB = getScopeOrder(b.labels);
    
    if (scopeOrderA !== scopeOrderB) {
      return scopeOrderA - scopeOrderB;
    }
    
    return a.title < b.title ? -1 : 1;
  })
  .reduce(
    (changelog, item) => {
      if (hasLabel(item, 'area/mimir')) {
        changelog.mimir.push(item);
      } else if (hasLabel(item, 'area/mixin')) {
        changelog.mixin.push(item);
      } else if (hasLabel(item, 'area/jsonnet')) {
        changelog.jsonnet.push(item);
      } else if (hasLabel(item, 'area/mimirtool')) {
        changelog.mimirtool.push(item);
      } else if (hasLabel(item, 'area/continuous-test')) {
        changelog.conttest.push(item);
      } else if (hasLabel(item, 'area/query-tee')) {
        changelog.querytee.push(item);
      } else if (hasLabel(item, 'area/docs')) {
        changelog.docs.push(item);
      } else if (hasLabel(item, 'area/tools')) {
        changelog.tools.push(item);
      }
      return changelog;
    },
    {
      mimir: [],
      mixin: [],
      jsonnet: [],
      mimirtool: [],
      conttest: [],
      querytee: [],
      docs: [],
      tools: [],
    }
  );

// Now that we have a changelog - we can render some markdown as an output
const markdown = (changelog) => {
  // This converts a list of changelog items into a markdown section
  const listItems = (items) => items
    .map(
      (item) =>
        `* [${getScope(item.labels)}] ${item.title.replace(/^([^:]*:)/gm, '$1')}. #${item.number}`
    )
    .join('\n');

  const section = (title, items) => {
    if (!items || items.length === 0) {
      if (!isMajorVersion) {
        return '';
      }
      return `### ${title}`;
    }
    return `### ${title}

${listItems(items)}`;
  }

  // Render all sections for the given changelog if it is a major version, else skip empty sections
  const sections = [
    ['Grafana Mimir', changelog.mimir],
    ['Mixin', changelog.mixin],
    ['Jsonnet', changelog.jsonnet],
    ['Mimirtool', changelog.mimirtool],
    ['Mimir Continuous Test', changelog.conttest],
    ['Query-tee', changelog.querytee],
    ['Documentation', changelog.docs],
    ['Tools', changelog.tools]
  ];

  return sections
    .map(([title, items]) => section(title, items))
    .filter(s => s !== '')
    .join('\n\n');
};

const md = markdown(changelog);

// Print changelog, mostly for debugging
LOG(`Resulting markdown: ${md}`);

// Save changelog as an output for this action
if (process.env.GITHUB_OUTPUT) {
  LOG(`Output to ${process.env.GITHUB_OUTPUT}`);
  appendFileSync(process.env.GITHUB_OUTPUT, `changelog<<EOF\n${escapeData(md)}\nEOF`);
} else {
  LOG('GITHUB_OUTPUT is not set');
}

// Save changelog as an output file (if requested)
if (process.env.INPUT_OUTPUT_FILE) {
  LOG(`Output to ${process.env.INPUT_OUTPUT_FILE}`);
  writeFileSync(process.env.INPUT_OUTPUT_FILE, md);
}
