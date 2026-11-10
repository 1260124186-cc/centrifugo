package historyrestore

import (
	"testing"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

func testPackage() *Package {
	return NewPackage("ch1", "epoch-1", 3, []Record{
		{
			Offset: 1,
			Epoch:  "epoch-1",
			Data:   []byte(`{"n":1}`),
			Tags:   map[string]string{"k": "v"},
			Info: &centrifuge.ClientInfo{
				ClientID: "client-1",
				UserID:   "user-1",
				ConnInfo: []byte(`{"a":1}`),
			},
		},
		{Offset: 2, Epoch: "epoch-1", Data: []byte(`{"n":2}`)},
		{Offset: 3, Epoch: "epoch-1", Data: []byte(`{"n":3}`), Tags: map[string]string{"z": "1", "a": "2"}},
	})
}

func TestSignAndValidate(t *testing.T) {
	pkg := testPackage()
	require.Equal(t, PackageVersion, pkg.Version)
	require.NotEmpty(t, pkg.Checksum)
	require.NoError(t, Validate(pkg))

	// The checksum is deterministic for the same content.
	require.Equal(t, ComputeChecksum(pkg), pkg.Checksum)
}

func TestValidateEmptyPackage(t *testing.T) {
	pkg := NewPackage("ch1", "", 0, nil)
	require.NoError(t, Validate(pkg))
	require.Empty(t, pkg.Publications)
}

func TestValidateCorrupted(t *testing.T) {
	pkg := testPackage()

	// Missing checksum.
	noChecksum := *pkg
	noChecksum.Checksum = ""
	require.ErrorIs(t, Validate(&noChecksum), ErrCorrupted)

	// Tampered payload: checksum no longer matches.
	tampered := *pkg
	tampered.Publications = append([]Record(nil), pkg.Publications...)
	tampered.Publications[1].Data = []byte(`{"n":999}`)
	require.ErrorIs(t, Validate(&tampered), ErrCorrupted)

	// Tampered top-level field.
	tamperedTop := *pkg
	tamperedTop.Offset = 99
	require.ErrorIs(t, Validate(&tamperedTop), ErrCorrupted)

	// Nil package.
	require.ErrorIs(t, Validate(nil), ErrInvalid)
}

func TestValidateStructural(t *testing.T) {
	pkg := testPackage()
	sign := func(p *Package) { Sign(p) }

	// Unsupported version.
	p := *pkg
	p.Version = 42
	p.Checksum = ComputeChecksum(&p)
	require.ErrorIs(t, Validate(&p), ErrInvalid)

	// Empty channel.
	p = *pkg
	p.Channel = ""
	p.Checksum = ComputeChecksum(&p)
	require.ErrorIs(t, Validate(&p), ErrInvalid)

	// Zero offset.
	bad := NewPackage("ch1", "epoch-1", 1, []Record{
		{Offset: 0, Epoch: "epoch-1", Data: []byte("x")},
	})
	require.ErrorIs(t, Validate(bad), ErrInvalid)

	// Offset beyond package top.
	bad = NewPackage("ch1", "epoch-1", 2, []Record{
		{Offset: 1, Epoch: "epoch-1", Data: []byte("x")},
		{Offset: 3, Epoch: "epoch-1", Data: []byte("y")},
	})
	require.ErrorIs(t, Validate(bad), ErrInvalid)

	// Duplicate / non-increasing offsets.
	dup := NewPackage("ch1", "epoch-1", 2, []Record{
		{Offset: 1, Epoch: "epoch-1", Data: []byte("x")},
		{Offset: 1, Epoch: "epoch-1", Data: []byte("y")},
	})
	require.ErrorIs(t, Validate(dup), ErrInvalid)

	// Record epoch differs from package epoch.
	wrongEpoch := NewPackage("ch1", "epoch-1", 1, []Record{
		{Offset: 1, Epoch: "other", Data: []byte("x")},
	})
	require.ErrorIs(t, Validate(wrongEpoch), ErrInvalid)

	// Well-formed again.
	sign(pkg)
	require.NoError(t, Validate(pkg))
}

func TestEqualTags(t *testing.T) {
	require.True(t, EqualTags(nil, nil))
	require.True(t, EqualTags(nil, map[string]string{}))
	require.True(t, EqualTags(map[string]string{"a": "1"}, map[string]string{"a": "1"}))
	require.False(t, EqualTags(map[string]string{"a": "1"}, map[string]string{"a": "2"}))
	require.False(t, EqualTags(map[string]string{"a": "1"}, map[string]string{"b": "1"}))
}

func TestEqualInfo(t *testing.T) {
	require.True(t, EqualInfo(nil, nil))
	require.False(t, EqualInfo(nil, &centrifuge.ClientInfo{}))
	require.False(t, EqualInfo(&centrifuge.ClientInfo{}, nil))
	a := &centrifuge.ClientInfo{ClientID: "c", UserID: "u", ConnInfo: []byte("x"), ChanInfo: []byte("y")}
	b := &centrifuge.ClientInfo{ClientID: "c", UserID: "u", ConnInfo: []byte("x"), ChanInfo: []byte("y")}
	require.True(t, EqualInfo(a, b))
	b.ConnInfo = []byte("z")
	require.False(t, EqualInfo(a, b))
}
