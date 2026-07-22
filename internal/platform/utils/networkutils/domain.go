package networkutils

import "context"

type DomainResolver interface {
	LookupHost(ctx context.Context, domain string) (addrs []string, err error)
}
