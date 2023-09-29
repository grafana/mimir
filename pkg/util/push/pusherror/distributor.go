package pusherror

type TenantLimit interface {
	IsTenantLimit()
}

func NewTenantLimitError(err error) error {
	return TenantLimitError{err}
}

type TenantLimitError struct {
	error
}

func (TenantLimitError) IsTenantLimit() {}
