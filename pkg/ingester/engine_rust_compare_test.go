// SPDX-License-Identifier: AGPL-3.0-only
//go:build enginecompare && rustengine && cgo

package ingester

func init() {
	compareSettle = func(i *Ingester, tenants []string) map[string]float64 {
		out := map[string]float64{}
		for n, tenant := range tenants {
			db := i.getTSDB(tenant)
			if db == nil {
				continue
			}
			e, ok := db.db.(*rustEngine)
			if !ok {
				continue
			}
			e.native.Purge()
			mem, err := e.native.Memory()
			if err != nil {
				continue
			}
			// The allocator's figures are the process', not the tenant's.
			if n == 0 {
				out["rust-allocated-B/series"] = float64(mem[0])
				out["rust-resident-B/series"] = float64(mem[1])
			}
			out["rust-series-B/series"] += float64(mem[2])
			out["rust-symbols-B/series"] += float64(mem[3])
			out["rust-postings-B/series"] += float64(mem[4])
			out["rust-slots-B/series"] += float64(mem[5])
		}
		return out
	}
}
