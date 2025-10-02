def flattenBlocks:
  if type == "object" and has("blockEntries") then
    .blockEntries[] | flattenBlocks
  elif type == "object" and .kind == "field" and .fieldFlag != null then
    .
  else
    empty
  end;

[flattenBlocks] | map(select(.fieldFlag and .fieldDefaultValue)) | 
  map({(.fieldFlag): .fieldDefaultValue}) | add