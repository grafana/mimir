# Cortex-Tool

This tool is designed to interact with the various user facing apis provided by cortex, as well as, interact with various backend storage components containing cortex data.

## Config Commands

Config commands interact with the cortex api and read/create/update/delete user configs from cortex. Specifically a users alertmanager and rule configs can be composed and updated using these commands.

### Configuration

| Variable          | Flag      | Description                                                                                                   |
| ----------------- | --------- | ------------------------------------------------------------------------------------------------------------- |
| CORTEX_ADDRESS    | --address | Addess of the api of the desired cortex cluster.                                                              |
| CORTEX_API_KEY    | --key     | In cases where the cortex api is set behind a basic auth gateway, an key can be set as a basic auth password. |
| CORTEX_TENTANT_ID | --id      | The tenant ID of the cortex instance to interact with.                                                        |

### Alertmanager

The following commands are used by users to interact with their cortex alertmanager configuration, as well as their alert template files.

#### Alertmanager Get

    cortex-cli alertmanager get

#### Alertmanager Load

    cortex-cli alertmanager load ./example_alertmanager_config.yaml

### Rules

The following commands are used by users to interact with their cortex ruler configuration. They can load prometheus rule files, as well as interact with individual rule groups.

#### Rules List

    cortex-cli rules list

#### RulesGet

    cortex-cli rules get example_rule_group

#### Rules Load

    cortex-cli rules load ./example_rules_one.yaml ./example_rules_two.yaml  ...

## Chunks

### Chunk Delete

The delete command currently cleans all index entries pointing to chunks in the specified index. Only bigtable and the v10 schema are currently fully supported. This will not delete the entire index entry, only the corresponding chunk entries within the index row.

## License

Licensed Apache 2.0, see [LICENSE](LICENSE).
