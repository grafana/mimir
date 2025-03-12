Completely naive string encoding:
Bins Total Struct Size MiB: 29651
Bins Total Compressed Struct Size MiB: 20826

With conversion for blockID into int64:
Bins Total Struct Size MiB: 16641
Bins Total Compressed Struct Size MiB: 7244

with int32 conversion for tenantID
```go
    blockID := filePathParts[len(filePathParts)-1]
    convertedBlockIDBytes := int64(binary.BigEndian.Uint64([]byte(blockID)))

    tenantIDBytes := []byte(filePathParts[len(filePathParts)-2])
    tenantIDFixedSizeBytes := make([]byte, 32)

    _, err = binary.Encode(tenantIDFixedSizeBytes, binary.BigEndian, tenantIDBytes)
    if err != nil {
        panic(err)``
    }

    convertedTenantID := int32(binary.LittleEndian.Uint32(tenantIDFixedSizeBytes))
```

Bins Total Struct Size MiB: 14467
Bins Total Compressed Struct Size MiB: 6405

Symbolizing the strings actually does worse, there are so many
Bins Total Struct Size MiB: 12248
Bins Total Compressed Struct Size MiB: 6721

Applying Lookup Table *AND* encoding tenantId as uint32
Bins Total Struct Size MiB: 10128
Bins Total Compressed Struct Size MiB: 4978 (Essentially 5 GiB)

Conclusions:
Too much data for memberlist or really for any of the relevant components to transfer and/or hold in memory.
