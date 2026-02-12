// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestAdvisoryLockUnlockReturns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params := base.TestServerArgs{}
	s, _, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	conn := s.SQLConn(t, serverutils.DBName("defaultdb"))
	defer conn.Close()
	db := sqlutils.MakeSQLRunner(conn)

	t.Run("Unlock Not Held Single Int", func(t *testing.T) {
		var released bool
		// Unlock a lock that was never acquired
		db.QueryRow(t, "SELECT pg_advisory_unlock(99999)").Scan(&released)
		require.False(t, released, "Should return false when unlocking a lock not held")
		
		db.QueryRow(t, "SELECT pg_advisory_unlock_shared(99999)").Scan(&released)
		require.False(t, released, "Should return false when unlocking a shared lock not held")
	})

	t.Run("Unlock Not Held Double Int", func(t *testing.T) {
		var released bool
		// Unlock a lock that was never acquired
		db.QueryRow(t, "SELECT pg_advisory_unlock(123, 456)").Scan(&released)
		require.False(t, released, "Should return false when unlocking a lock not held")
		
		db.QueryRow(t, "SELECT pg_advisory_unlock_shared(123, 456)").Scan(&released)
		require.False(t, released, "Should return false when unlocking a shared lock not held")
	})

	t.Run("Unlock Held Returns True", func(t *testing.T) {
		db.Exec(t, "SELECT pg_advisory_lock(88888)")
		var released bool
		db.QueryRow(t, "SELECT pg_advisory_unlock(88888)").Scan(&released)
		require.True(t, released, "Should return true when unlocking a held lock")

		// Unlock again should return false
		db.QueryRow(t, "SELECT pg_advisory_unlock(88888)").Scan(&released)
		require.False(t, released, "Should return false when unlocking an already unlocked lock")
	})
}
