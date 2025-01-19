package ring

const ACTIVE = InstanceState_ACTIVE
const LEAVING = InstanceState_LEAVING
const PENDING = InstanceState_PENDING
const JOINING = InstanceState_JOINING
const LEFT = InstanceState_LEFT

func (this *InstanceDesc) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	that1, ok := that.(*InstanceDesc)
	if !ok {
		that2, ok := that.(InstanceDesc)
		if ok {
			that1 = &that2
		} else {
			return false
		}
	}
	if that1 == nil {
		return this == nil
	} else if this == nil {
		return false
	}
	if this.Addr != that1.Addr {
		return false
	}
	if this.Timestamp != that1.Timestamp {
		return false
	}
	if this.State != that1.State {
		return false
	}
	if len(this.Tokens) != len(that1.Tokens) {
		return false
	}
	for i := range this.Tokens {
		if this.Tokens[i] != that1.Tokens[i] {
			return false
		}
	}
	if this.Zone != that1.Zone {
		return false
	}
	if this.RegisteredTimestamp != that1.RegisteredTimestamp {
		return false
	}
	if this.Id != that1.Id {
		return false
	}
	if this.ReadOnlyUpdatedTimestamp != that1.ReadOnlyUpdatedTimestamp {
		return false
	}
	if this.ReadOnly != that1.ReadOnly {
		return false
	}
	return true
}
