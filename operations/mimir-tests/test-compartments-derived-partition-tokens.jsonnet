local derivedTokensTest = import 'test-ingest-storage-derived-partition-tokens.jsonnet';

local env = (import 'test-compartments.jsonnet') + derivedTokensTest.derivedTokensConfig;
local errors = derivedTokensTest.derivedTokensErrors(env);
assert std.length(errors) == 0 : std.join('; ', errors);

env
