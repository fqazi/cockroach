// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package lease

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/errors"
)

// descriptorSet maintains an ordered set of descriptorVersionState objects
// sorted by version. It supports addition and removal of elements, finding the
// descriptor for a particular version, or finding the most recent version.
// The order is maintained by insert and remove and there can only be a
// unique entry for a version. Only the last two versions can be leased,
// with the last one being the latest one which is always leased.
//
// Each entry represents a time span [ModificationTime, expiration)
// and can be used by a transaction iif:
// ModificationTime <= transaction.Timestamp < expiration.
type descriptorSet struct {
	data   atomic.Pointer[dataRefWrapper]
	mutate sync.Mutex
	//data []*descriptorVersionState
}

type dataRefWrapper struct {
	data []*descriptorVersionState
	ref  atomic.Int32
}

func (l *descriptorSet) withData(fn func([]*descriptorVersionState) error) error {
	if d := l.data.Load(); d != nil {
		d.ref.Add(1)
		defer d.ref.Add(-1)
		return fn(d.data)
	}
	return fn(nil)
}

func (l *descriptorSet) mutateData(f func([]*descriptorVersionState) []*descriptorVersionState) {
	l.mutate.Lock()
	defer l.mutate.Unlock()
	old := l.data.Load()
	new := &dataRefWrapper{}
	if old != nil {
		new.data = make([]*descriptorVersionState, len(old.data))
		copy(new.data, old.data)
	}
	new.data = f(new.data)
	l.data.Store(new)
	// Wait for the old ref count to hit zero.
	if old != nil {
		for old.ref.Load() > 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (l *descriptorSet) String() string {
	var buf bytes.Buffer
	_ = l.withData(func(data []*descriptorVersionState) error {
		for i, s := range data {
			if i > 0 {
				buf.WriteString(" ")
			}
			buf.WriteString(fmt.Sprintf("%d:%d", s.GetVersion(), s.getExpiration(context.TODO()).WallTime))
		}
		return nil
	})
	return buf.String()
}

func (l *descriptorSet) insert(s *descriptorVersionState) {
	l.mutateData(func(data []*descriptorVersionState) []*descriptorVersionState {
		i, match := l.findIndex(s.GetVersion())
		if match {
			panic("unable to insert duplicate lease")
		}
		if i == len(data) {
			data = append(data, s)
			return data
		}
		data = append(data, nil)
		copy(data[i+1:], data[i:])
		data[i] = s
		return data
	})
}

func (l *descriptorSet) remove(s *descriptorVersionState) {
	l.mutateData(func(data []*descriptorVersionState) []*descriptorVersionState {
		i, match := l.findIndex(s.GetVersion())
		if !match {
			panic(errors.AssertionFailedf("can't find lease to remove: %s", s))
		}
		data = append(data[:i], data[i+1:]...)
		return data
	})
}

func (l *descriptorSet) find(version descpb.DescriptorVersion) (res *descriptorVersionState) {
	_ = l.withData(func(data []*descriptorVersionState) error {
		if i, match := l.findIndexWithData(data, version); match {
			res = data[i]
		}
		return nil
	})
	return res
}

func (l *descriptorSet) findIndexWithData(
	data []*descriptorVersionState, version descpb.DescriptorVersion,
) (int, bool) {
	i := sort.Search(len(data), func(i int) bool {
		s := data[i]
		return s.GetVersion() >= version
	})
	if i < len(data) {
		s := data[i]
		if s.GetVersion() == version {
			return i, true
		}
	}
	return i, false
}

func (l *descriptorSet) findIndex(version descpb.DescriptorVersion) (res int, found bool) {
	_ = l.withData(func(data []*descriptorVersionState) error {
		res, found = l.findIndexWithData(data, version)
		return nil
	})
	return res, found
}

func (l *descriptorSet) findNewest() (res *descriptorVersionState) {
	_ = l.withData(func(data []*descriptorVersionState) error {
		if len(data) == 0 {
			return nil
		}
		res = data[len(data)-1]
		return nil
	})
	return res
}

func (l *descriptorSet) findPreviousToExpire(dropped bool) (exp *descriptorVersionState) {
	_ = l.withData(func(data []*descriptorVersionState) error {
		// If there are no versions, then no previous version exists.
		if len(data) == 0 {
			return nil
		}
		// The latest version will be the previous version
		// if the descriptor is dropped.
		exp = data[len(data)-1]
		if len(data) > 1 && !dropped {
			// Otherwise, the second last element will be the previous version.
			exp = data[len(data)-2]
		} else if !dropped {
			// Otherwise, there is a single non-dropped element
			// avoid expiring.
			exp = nil
			return nil
		}
		// If the refcount has hit zero then this version will be cleaned up
		// automatically. If an expiration time is already setup, then this
		// version has already been expired.
		if exp.refcount.Load() == 0 || (exp.expiration.Load() != nil) {
			exp = nil
		}
		return nil
	})
	return exp
}

func (l *descriptorSet) findVersion(
	version descpb.DescriptorVersion,
) (res *descriptorVersionState) {
	_ = l.withData(func(data []*descriptorVersionState) error {
		if len(data) == 0 {
			return nil
		}
		// Find the index of the first lease with version > targetVersion.
		i := sort.Search(len(data), func(i int) bool {
			return data[i].GetVersion() > version
		})
		if i == 0 {
			return nil
		}
		// i-1 is the index of the newest lease for the previous version (the version
		// we're looking for).
		s := data[i-1]
		if s.GetVersion() == version {
			res = s
		}
		return nil
	})
	return res
}

func (l *descriptorSet) isEmpty() bool {
	var empty bool
	_ = l.withData(func(data []*descriptorVersionState) error {
		empty = len(data) == 0
		return nil
	})
	return empty
}
