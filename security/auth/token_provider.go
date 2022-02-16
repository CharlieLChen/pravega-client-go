package auth

type DelegationTokenProvider interface {
	retrieveToken() string
	populateToken(token string) bool
	signalTokenExpired()
}
type EmptyDelegationTokenProvider struct {
}

func (provider *EmptyDelegationTokenProvider) retrieveToken() string {
	return ""
}
func (provider *EmptyDelegationTokenProvider) populateToken(token string) bool {
	return false
}
func (provider *EmptyDelegationTokenProvider) signalTokenExpired() {

}
