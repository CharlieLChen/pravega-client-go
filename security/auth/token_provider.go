package auth

type DelegationTokenProvider interface {
	RetrieveToken() string
	PopulateToken(token string) bool
	SignalTokenExpired()
}
type EmptyDelegationTokenProvider struct {
}

func (provider *EmptyDelegationTokenProvider) RetrieveToken() string {
	return ""
}
func (provider *EmptyDelegationTokenProvider) PopulateToken(token string) bool {
	return false
}
func (provider *EmptyDelegationTokenProvider) SignalTokenExpired() {

}
