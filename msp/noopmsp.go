package msp

import (
	"time"

	"github.com/hyperledger/fabric/protos/msp"
)

// noopmsp is the mock MSP implementation
type noopmsp struct {
	wrappedMsp *bccspmsp
}

// newNoopMsp returns a no-op implementation of the MSP interface
func newNoopMsp(mInst *bccspmsp) MSP {
	return &noopmsp{mInst}
}

func (m *noopmsp) Setup(conf1 *msp.MSPConfig) error {
	return m.wrappedMsp.Setup(conf1)
}

func (m *noopmsp) GetVersion() MSPVersion {
	return MSPv1_0
}

func (m *noopmsp) GetType() ProviderType {
	return 0
}

func (m *noopmsp) GetIdentifier() (string, error) {
	return m.wrappedMsp.GetIdentifier()
}

func (m *noopmsp) GetSigningIdentity(identifier *IdentityIdentifier) (SigningIdentity, error) {
	id, _ := newNoopSigningIdentity()
	return id, nil
}

func (m *noopmsp) GetDefaultSigningIdentity() (SigningIdentity, error) {
	id, _ := newNoopSigningIdentity()
	return id, nil
}

// GetRootCerts returns the root certificates for this MSP
func (m *noopmsp) GetRootCerts() []Identity {
	return nil
}

// GetIntermediateCerts returns the intermediate root certificates for this MSP
func (m *noopmsp) GetIntermediateCerts() []Identity {
	return nil
}

// GetTLSRootCerts returns the root certificates for this MSP
func (m *noopmsp) GetTLSRootCerts() [][]byte {
	return m.wrappedMsp.GetTLSRootCerts()
}

// GetTLSIntermediateCerts returns the intermediate root certificates for this MSP
func (m *noopmsp) GetTLSIntermediateCerts() [][]byte {
	return m.wrappedMsp.GetTLSIntermediateCerts()
}

func (m *noopmsp) DeserializeIdentity(serializedID []byte) (Identity, error) {
	id, _ := newNoopIdentity()
	return id, nil
}

func (m *noopmsp) Validate(id Identity) error {
	return nil
}

func (m *noopmsp) SatisfiesPrincipal(id Identity, principal *msp.MSPPrincipal) error {
	return nil
}

// IsWellFormed checks if the given identity can be deserialized into its provider-specific form
func (m *noopmsp) IsWellFormed(_ *msp.SerializedIdentity) error {
	return nil
}

type noopidentity struct {
}

func newNoopIdentity() (Identity, error) {
	return &noopidentity{}, nil
}

func (id *noopidentity) Anonymous() bool {
	return true
}

func (id *noopidentity) SatisfiesPrincipal(*msp.MSPPrincipal) error {
	return nil
}

func (id *noopidentity) ExpiresAt() time.Time {
	return time.Time{}
}

func (id *noopidentity) GetIdentifier() *IdentityIdentifier {
	return &IdentityIdentifier{Mspid: "NOOP", Id: "Bob"}
}

func (id *noopidentity) GetMSPIdentifier() string {
	return "MSPID"
}

func (id *noopidentity) Validate() error {
	return nil
}

func (id *noopidentity) GetOrganizationalUnits() []*OUIdentifier {
	return nil
}

func (id *noopidentity) Verify(msg []byte, sig []byte) error {
	return nil
}

func (id *noopidentity) Serialize() ([]byte, error) {
	return []byte("cert"), nil
}

type noopsigningidentity struct {
	noopidentity
}

func newNoopSigningIdentity() (SigningIdentity, error) {
	return &noopsigningidentity{}, nil
}

func (id *noopsigningidentity) Sign(msg []byte) ([]byte, error) {
	return []byte("signature"), nil
}

func (id *noopsigningidentity) GetPublicVersion() Identity {
	return id
}
