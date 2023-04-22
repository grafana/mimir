package tokendistributor

func createRingTokensInstancesZones() ([]Token, map[Token]Instance, map[Instance]Zone) {
	sortedRingTokens := []Token{48, 97, 194, 285, 380, 476, 572, 668, 736, 804, 853, 902}
	ringInstanceByToken := map[Token]Instance{
		48:  "instance-2",
		97:  "instance-1",
		194: "instance-0",
		285: "instance-0",
		380: "instance-2",
		476: "instance-1",
		572: "instance-2",
		668: "instance-0",
		736: "instance-1",
		804: "instance-0",
		853: "instance-1",
		902: "instance-2",
	}
	zoneByInstance := map[Instance]Zone{
		"instance-0": "zone-a",
		"instance-1": "zone-b",
		"instance-2": "zone-c",
	}
	return sortedRingTokens, ringInstanceByToken, zoneByInstance
}
