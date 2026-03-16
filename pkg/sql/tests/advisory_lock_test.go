// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestAdvisoryLocks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Support is not there for all lock commands.
	//skip.WithIssue(t, 12345)

	ctx := context.Background()
	params := base.TestServerArgs{}
	s, _, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Create two distinct connections to simulate two sessions.
	conn1 := s.SQLConn(t, serverutils.DBName("defaultdb"))
	defer conn1.Close()
	db1 := sqlutils.MakeSQLRunner(conn1)

	conn2 := s.SQLConn(t, serverutils.DBName("defaultdb"))
	defer conn2.Close()
	db2 := sqlutils.MakeSQLRunner(conn2)

	t.Run("Session Locks Single Int", func(t *testing.T) {
		// Session 1 acquires lock
		db1.Exec(t, "SELECT pg_advisory_lock(1)")

		// Session 2 tries to acquire same lock, should fail
		var acquired bool
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(1)").Scan(&acquired)
		require.False(t, acquired, "Session 2 should not acquire lock held by Session 1")

		// Session 1 releases lock
		var released bool
		db1.QueryRow(t, "SELECT pg_advisory_unlock(1)").Scan(&released)
		require.True(t, released, "Unlock should return true when lock was held")

		// Session 2 tries to acquire again, should succeed
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(1)").Scan(&acquired)
		require.True(t, acquired, "Session 2 should acquire lock after release")

		// Session 2 releases lock
		db2.QueryRow(t, "SELECT pg_advisory_unlock(1)").Scan(&released)
		require.True(t, released)
	})

	t.Run("Session Locks Double Int", func(t *testing.T) {
		// Session 1 acquires lock
		db1.Exec(t, "SELECT pg_advisory_lock(1, 2)")

		// Session 2 tries to acquire same lock, should fail
		var acquired bool
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(1, 2)").Scan(&acquired)
		require.False(t, acquired, "Session 2 should not acquire lock held by Session 1")

		// Session 1 releases lock
		var released bool
		db1.QueryRow(t, "SELECT pg_advisory_unlock(1, 2)").Scan(&released)
		require.True(t, released, "Unlock should return true when lock was held")

		// Session 2 tries to acquire again, should succeed
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(1, 2)").Scan(&acquired)
		require.True(t, acquired, "Session 2 should acquire lock after release")

		// Session 2 releases lock
		db2.QueryRow(t, "SELECT pg_advisory_unlock(1, 2)").Scan(&released)
		require.True(t, released)
	})

	t.Run("Shared Locks", func(t *testing.T) {
		// Session 1 acquires shared lock
		db1.Exec(t, "SELECT pg_advisory_lock_shared(10)")

		// Session 2 acquires shared lock (should succeed)
		var acquired bool
		db2.QueryRow(t, "SELECT pg_try_advisory_lock_shared(10)").Scan(&acquired)
		require.True(t, acquired, "Session 2 should acquire shared lock even if Session 1 holds it")

		// Session 2 tries to acquire exclusive lock (should fail)
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(10)").Scan(&acquired)
		require.False(t, acquired, "Session 2 should not acquire exclusive lock if shared lock is held")

		// Cleanup
		db1.Exec(t, "SELECT pg_advisory_unlock_shared(10)")
		db2.Exec(t, "SELECT pg_advisory_unlock_shared(10)")
	})

	t.Run("Shared Locks Double Int", func(t *testing.T) {
		// Session 1 acquires shared lock
		db1.Exec(t, "SELECT pg_advisory_lock_shared(10, 20)")

		// Session 2 acquires shared lock (should succeed)
		var acquired bool
		db2.QueryRow(t, "SELECT pg_try_advisory_lock_shared(10, 20)").Scan(&acquired)
		require.True(t, acquired, "Session 2 should acquire shared lock even if Session 1 holds it")

		// Session 2 tries to acquire exclusive lock (should fail)
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(10, 20)").Scan(&acquired)
		require.False(t, acquired, "Session 2 should not acquire exclusive lock if shared lock is held")

		// Cleanup
		db1.Exec(t, "SELECT pg_advisory_unlock_shared(10, 20)")
		db2.Exec(t, "SELECT pg_advisory_unlock_shared(10, 20)")
	})

	t.Run("Transaction Locks", func(t *testing.T) {
		// Session 1 starts txn and acquires xact lock
		db1.Exec(t, "BEGIN")
		db1.Exec(t, "SELECT pg_advisory_xact_lock(20)")

		// Session 2 tries to acquire lock (should fail)
		var acquired bool
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(20)").Scan(&acquired)
		require.False(t, acquired)

		// Session 1 commits
		db1.Exec(t, "COMMIT")

		// Session 2 tries to acquire lock (should succeed as xact lock released on commit)
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(20)").Scan(&acquired)
		require.True(t, acquired)

		// Cleanup
		db2.Exec(t, "SELECT pg_advisory_unlock(20)")
	})

	t.Run("Transaction Locks Double Int", func(t *testing.T) {
		// Session 1 starts txn and acquires xact lock
		db1.Exec(t, "BEGIN")
		db1.Exec(t, "SELECT pg_advisory_xact_lock(20, 30)")

		// Session 2 tries to acquire lock (should fail)
		var acquired bool
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(20, 30)").Scan(&acquired)
		require.False(t, acquired)

		// Session 1 commits
		db1.Exec(t, "COMMIT")

		// Session 2 tries to acquire lock (should succeed as xact lock released on commit)
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(20, 30)").Scan(&acquired)
		require.True(t, acquired)

		// Cleanup
		db2.Exec(t, "SELECT pg_advisory_unlock(20, 30)")
	})

	t.Run("Transaction Shared Locks", func(t *testing.T) {
		// Session 1 starts txn and acquires shared xact lock
		db1.Exec(t, "BEGIN")
		db1.Exec(t, "SELECT pg_advisory_xact_lock_shared(40)")

		// Session 2 tries to acquire exclusive lock (should fail)
		var acquired bool
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(40)").Scan(&acquired)
		require.False(t, acquired)

		// Session 2 tries to acquire shared lock (should succeed)
		db2.QueryRow(t, "SELECT pg_try_advisory_lock_shared(40)").Scan(&acquired)
		require.True(t, acquired)
		db2.Exec(t, "SELECT pg_advisory_unlock_shared(40)") // release session lock

		// Session 1 commits
		db1.Exec(t, "COMMIT")

		// Session 2 tries to acquire exclusive lock (should succeed now)
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(40)").Scan(&acquired)
		require.True(t, acquired)
		db2.Exec(t, "SELECT pg_advisory_unlock(40)")
	})

	t.Run("Transaction Shared Locks Double Int", func(t *testing.T) {
		// Session 1 starts txn and acquires shared xact lock
		db1.Exec(t, "BEGIN")
		db1.Exec(t, "SELECT pg_advisory_xact_lock_shared(40, 50)")

		// Session 2 tries to acquire exclusive lock (should fail)
		var acquired bool
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(40, 50)").Scan(&acquired)
		require.False(t, acquired)

		// Session 2 tries to acquire shared lock (should succeed)
		db2.QueryRow(t, "SELECT pg_try_advisory_lock_shared(40, 50)").Scan(&acquired)
		require.True(t, acquired)
		db2.Exec(t, "SELECT pg_advisory_unlock_shared(40, 50)") // release session lock

		// Session 1 commits
		db1.Exec(t, "COMMIT")

		// Session 2 tries to acquire exclusive lock (should succeed now)
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(40, 50)").Scan(&acquired)
		require.True(t, acquired)
		db2.Exec(t, "SELECT pg_advisory_unlock(40, 50)")
	})

	t.Run("Try Transaction Locks", func(t *testing.T) {
		// Test pg_try_advisory_xact_lock(int)
		db1.Exec(t, "BEGIN")
		var acquired bool
		db1.QueryRow(t, "SELECT pg_try_advisory_xact_lock(60)").Scan(&acquired)
		require.True(t, acquired)

		// Verify held
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(60)").Scan(&acquired)
		require.False(t, acquired)

		db1.Exec(t, "COMMIT")
		// Verify released
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(60)").Scan(&acquired)
		require.True(t, acquired)
		db2.Exec(t, "SELECT pg_advisory_unlock(60)")

		// Test pg_try_advisory_xact_lock(int, int)
		db1.Exec(t, "BEGIN")
		db1.QueryRow(t, "SELECT pg_try_advisory_xact_lock(60, 70)").Scan(&acquired)
		require.True(t, acquired)
		db1.Exec(t, "COMMIT")

		// Test pg_try_advisory_xact_lock_shared(int)
		db1.Exec(t, "BEGIN")
		db1.QueryRow(t, "SELECT pg_try_advisory_xact_lock_shared(80)").Scan(&acquired)
		require.True(t, acquired)
		// Should allow other shared
		db2.QueryRow(t, "SELECT pg_try_advisory_lock_shared(80)").Scan(&acquired)
		require.True(t, acquired)
		db2.Exec(t, "SELECT pg_advisory_unlock_shared(80)")
		db1.Exec(t, "COMMIT")

		// Test pg_try_advisory_xact_lock_shared(int, int)
		db1.Exec(t, "BEGIN")
		db1.QueryRow(t, "SELECT pg_try_advisory_xact_lock_shared(80, 90)").Scan(&acquired)
		require.True(t, acquired)
		db1.Exec(t, "COMMIT")
	})

	t.Run("Unlock All", func(t *testing.T) {
		db1.Exec(t, "SELECT pg_advisory_lock(100)")
		db1.Exec(t, "SELECT pg_advisory_lock(101)")

		db1.Exec(t, "SELECT pg_advisory_unlock_all()")

		var acquired bool
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(100)").Scan(&acquired)
		require.True(t, acquired)
		db2.Exec(t, "SELECT pg_advisory_unlock(100)")

		db2.QueryRow(t, "SELECT pg_try_advisory_lock(101)").Scan(&acquired)
		require.True(t, acquired)
		db2.Exec(t, "SELECT pg_advisory_unlock(101)")
	})

	t.Run("Try Locks", func(t *testing.T) {
		var acquired bool
		db1.QueryRow(t, "SELECT pg_try_advisory_lock(200)").Scan(&acquired)
		require.True(t, acquired)

		db2.QueryRow(t, "SELECT pg_try_advisory_lock(200)").Scan(&acquired)
		require.False(t, acquired)

		db1.Exec(t, "SELECT pg_advisory_unlock(200)")
	})

	t.Run("Mixed Overloads Isolation", func(t *testing.T) {
		// Verify that (1, 2) is distinct from (1)
		// Note: The implementation maps (k1, k2) to a 64-bit int.
		// (1) maps to 1.
		// (1, 2) maps to (1 << 32) | 2.
		// So they should be distinct.

		db1.Exec(t, "SELECT pg_advisory_lock(1)")

		var acquired bool
		// Should be able to acquire (1, 2) because it's a different lock ID
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(1, 2)").Scan(&acquired)
		require.True(t, acquired)

		db1.Exec(t, "SELECT pg_advisory_unlock(1)")
		db2.Exec(t, "SELECT pg_advisory_unlock(1, 2)")
	})

	t.Run("Contended Lock Acquisition", func(t *testing.T) {
		// Session 1 holds lock
		db1.Exec(t, "SELECT pg_advisory_lock(300)")

		done := make(chan struct{})
		go func() {
			defer close(done)
			// Session 2 tries to acquire lock, should block until Session 1 releases
			db2.Exec(t, "SELECT pg_advisory_lock(300)")
		}()

		// Ensure Session 2 is blocked (simple sleep)
		time.Sleep(100 * time.Millisecond)
		select {
		case <-done:
			t.Fatal("Session 2 acquired lock prematurely")
		default:
			// Expected to block
		}

		// Session 1 releases lock
		db1.Exec(t, "SELECT pg_advisory_unlock(300)")

		// Session 2 should now complete
		select {
		case <-done:
			// Success
		case <-time.After(5 * time.Second):
			t.Fatal("Session 2 timed out waiting for lock")
		}

		db2.Exec(t, "SELECT pg_advisory_unlock(300)")
	})

	t.Run("Contended Shared Lock Acquisition", func(t *testing.T) {
		// Session 1 holds Exclusive lock
		db1.Exec(t, "SELECT pg_advisory_lock(301)")

		done := make(chan struct{})
		go func() {
			defer close(done)
			// Session 2 tries to acquire Shared lock, should block until Session 1 releases
			db2.Exec(t, "SELECT pg_advisory_lock_shared(301)")
		}()

		// Ensure Session 2 is blocked
		time.Sleep(100 * time.Millisecond)
		select {
		case <-done:
			t.Fatal("Session 2 acquired shared lock prematurely")
		default:
			// Expected to block
		}

		// Session 1 releases lock
		db1.Exec(t, "SELECT pg_advisory_unlock(301)")

		// Session 2 should now complete
		select {
		case <-done:
			// Success
		case <-time.After(5 * time.Second):
			t.Fatal("Session 2 timed out waiting for shared lock")
		}

		db2.Exec(t, "SELECT pg_advisory_unlock_shared(301)")
	})

	t.Run("Shared Lock Non-Contention", func(t *testing.T) {
		// Session 1 holds Shared lock
		db1.Exec(t, "SELECT pg_advisory_lock_shared(302)")

		done := make(chan struct{})
		go func() {
			defer close(done)
			// Session 2 tries to acquire Shared lock, should NOT block
			db2.Exec(t, "SELECT pg_advisory_lock_shared(302)")
		}()

		select {
		case <-done:
			// Success, acquired immediately
		case <-time.After(2 * time.Second):
			t.Fatal("Session 2 blocked trying to acquire compatible shared lock")
		}

		db1.Exec(t, "SELECT pg_advisory_unlock_shared(302)")
		db2.Exec(t, "SELECT pg_advisory_unlock_shared(302)")
	})

	t.Run("Deadlock Detection", func(t *testing.T) {
		// Session 1 acquires Lock A
		db1.Exec(t, "SELECT pg_advisory_lock(400)")
		// Session 2 acquires Lock B
		db2.Exec(t, "SELECT pg_advisory_lock(401)")

		grpCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		grp := ctxgroup.WithContext(grpCtx)

		grp.GoCtx(func(ctx context.Context) error {
			defer cancel()
			_, err := db1.DB.ExecContext(ctx, "SELECT pg_advisory_lock(401)")
			return err
		})

		grp.GoCtx(func(ctx context.Context) error {
			defer cancel()
			// Session 2 tries to acquire Lock A (held by Session 1)
			// Wait a bit to ensure Session 1 is waiting first (force order for determinism if possible, though deadlock detector handles any order)
			_, err := db2.DB.ExecContext(ctx, "SELECT pg_advisory_lock(400)")
			return err
		})
		err := grp.Wait()
		deadlockFound := false
		if pqErr := (*pq.Error)(nil); errors.As(err, &pqErr) {
			if pgcode.MakeCode(string(pqErr.Code)) == pgcode.DeadlockDetected {
				deadlockFound = true
			}
		}
		require.True(t, deadlockFound, "Expected a deadlock error")

		// Cleanup: Reset sessions/locks might be needed if connections are poisoned,
		// but advisory locks usually persist unless session dies.
		// Deadlock error aborts current transaction/statement.
		// We try to release everything.
		db1.Exec(t, "SELECT pg_advisory_unlock_all()")
		db2.Exec(t, "SELECT pg_advisory_unlock_all()")
	})

	t.Run("Lock Upgrade", func(t *testing.T) {
		// Session 1 acquires Shared lock
		db1.Exec(t, "SELECT pg_advisory_lock_shared(500)")

		// Session 1 upgrades to Exclusive lock (should succeed immediately)
		db1.Exec(t, "SELECT pg_advisory_lock(500)")

		// Session 2 tries to acquire Shared lock (should fail/block, here we try non-blocking)
		var acquired bool
		db2.QueryRow(t, "SELECT pg_try_advisory_lock_shared(500)").Scan(&acquired)
		require.False(t, acquired, "Session 2 should not acquire shared lock because Session 1 holds exclusive")

		// Session 1 releases Exclusive lock
		var released bool
		db1.QueryRow(t, "SELECT pg_advisory_unlock(500)").Scan(&released)
		require.True(t, released)

		// Session 2 tries to acquire Shared lock (should succeed now because Session 1 only holds Shared)
		db2.QueryRow(t, "SELECT pg_try_advisory_lock_shared(500)").Scan(&acquired)
		require.True(t, acquired, "Session 2 should acquire shared lock (compatible with Session 1 Shared)")

		// Cleanup
		db1.Exec(t, "SELECT pg_advisory_unlock_shared(500)")
		db2.Exec(t, "SELECT pg_advisory_unlock_shared(500)")
	})

	t.Run("Lock Upgrade Deadlock", func(t *testing.T) {
		// Session 1 acquires Shared
		db1.Exec(t, "SELECT pg_advisory_lock_shared(501)")
		// Session 2 acquires Shared
		db2.Exec(t, "SELECT pg_advisory_lock_shared(501)")

		// Both sessions now hold Shared. Neither can upgrade to Exclusive without the other releasing.
		// If both try to upgrade, one must error with deadlock.
		grpCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		grp := ctxgroup.WithContext(grpCtx)

		grp.GoCtx(func(ctx context.Context) error {
			defer cancel()
			// Session 1 tries upgrade
			_, err := db1.DB.ExecContext(ctx, "SELECT pg_advisory_lock(501)")
			return err
		})

		grp.GoCtx(func(ctx context.Context) error {
			defer cancel()
			// Session 2 tries upgrade
			_, err := db2.DB.ExecContext(ctx, "SELECT pg_advisory_lock(501)")
			return err
		})

		err := grp.Wait()
		var deadlockFound bool
		if pqErr := (*pq.Error)(nil); errors.As(err, &pqErr) {
			if pgcode.MakeCode(string(pqErr.Code)) == pgcode.DeadlockDetected {
				deadlockFound = true
			}
		}
		require.True(t, deadlockFound, "Expected a deadlock error during mutual upgrade")

		// Cleanup
		db1.Exec(t, "SELECT pg_advisory_unlock_all()")
		db2.Exec(t, "SELECT pg_advisory_unlock_all()")
	})

	t.Run("Deadlock Winner Acquires Lock", func(t *testing.T) {
		// After a deadlock, the loser gets REASON_DEADLOCK and stops
		// waiting. The winner is still blocked on the loser's original
		// lock. Once the loser releases that lock, the winner should
		// acquire it (not hang forever).
		db1.Exec(t, "SELECT pg_advisory_lock(600)")
		db2.Exec(t, "SELECT pg_advisory_lock(601)")

		errCh1 := make(chan error, 1)
		errCh2 := make(chan error, 1)

		go func() {
			_, err := db1.DB.ExecContext(context.Background(), "SELECT pg_advisory_lock(601)")
			errCh1 <- err
		}()
		go func() {
			_, err := db2.DB.ExecContext(context.Background(), "SELECT pg_advisory_lock(600)")
			errCh2 <- err
		}()

		// Wait for the loser (first to return with a deadlock error).
		// Then release its locks to unblock the winner.
		select {
		case err := <-errCh1:
			// db1 is the loser.
			require.Error(t, err)
			db1.Exec(t, "SELECT pg_advisory_unlock_all()")
			// Winner (db2) should now acquire lock 600.
			require.NoError(t, <-errCh2, "Winner should acquire lock after loser releases")
		case err := <-errCh2:
			// db2 is the loser.
			require.Error(t, err)
			db2.Exec(t, "SELECT pg_advisory_unlock_all()")
			// Winner (db1) should now acquire lock 601.
			require.NoError(t, <-errCh1, "Winner should acquire lock after loser releases")
		case <-time.After(30 * time.Second):
			t.Fatal("Timed out waiting for deadlock detection")
		}

		// Cleanup
		db1.Exec(t, "SELECT pg_advisory_unlock_all()")
		db2.Exec(t, "SELECT pg_advisory_unlock_all()")
	})

	t.Run("Locks Survive Deadlock", func(t *testing.T) {
		// After a deadlock error, the losing session's advisory-lock-txn
		// should NOT be aborted. Previously held locks must still be valid
		// and the session must be able to acquire new locks.
		db1.Exec(t, "SELECT pg_advisory_lock(700)")
		db2.Exec(t, "SELECT pg_advisory_lock(701)")

		errCh1 := make(chan error, 1)
		errCh2 := make(chan error, 1)

		go func() {
			_, err := db1.DB.ExecContext(context.Background(), "SELECT pg_advisory_lock(701)")
			errCh1 <- err
		}()
		go func() {
			_, err := db2.DB.ExecContext(context.Background(), "SELECT pg_advisory_lock(700)")
			errCh2 <- err
		}()

		// Wait for the loser. Verify its advisory-lock-txn survived:
		// it can acquire a NEW lock and release its ORIGINAL lock.
		select {
		case err := <-errCh1:
			// db1 is the loser.
			require.Error(t, err)
			// Loser can acquire a new lock (advisory-lock-txn alive).
			db1.Exec(t, "SELECT pg_advisory_lock(702)")
			// Loser can release its original lock.
			var released bool
			db1.QueryRow(t, "SELECT pg_advisory_unlock(700)").Scan(&released)
			require.True(t, released, "Loser should still hold its original lock")
			// Releasing the original lock unblocks the winner.
			require.NoError(t, <-errCh2)
		case err := <-errCh2:
			// db2 is the loser.
			require.Error(t, err)
			db2.Exec(t, "SELECT pg_advisory_lock(702)")
			var released bool
			db2.QueryRow(t, "SELECT pg_advisory_unlock(701)").Scan(&released)
			require.True(t, released, "Loser should still hold its original lock")
			require.NoError(t, <-errCh1)
		case <-time.After(30 * time.Second):
			t.Fatal("Timed out waiting for deadlock detection")
		}

		// Cleanup
		db1.Exec(t, "SELECT pg_advisory_unlock_all()")
		db2.Exec(t, "SELECT pg_advisory_unlock_all()")
	})

	t.Run("Re-acquire After Deadlock", func(t *testing.T) {
		// After a deadlock, the loser should be able to retry and
		// successfully acquire the same lock once all locks are released.
		db1.Exec(t, "SELECT pg_advisory_lock(800)")
		db2.Exec(t, "SELECT pg_advisory_lock(801)")

		errCh1 := make(chan error, 1)
		errCh2 := make(chan error, 1)

		go func() {
			_, err := db1.DB.ExecContext(context.Background(), "SELECT pg_advisory_lock(801)")
			errCh1 <- err
		}()
		go func() {
			_, err := db2.DB.ExecContext(context.Background(), "SELECT pg_advisory_lock(800)")
			errCh2 <- err
		}()

		// Wait for loser, release its locks to unblock winner.
		select {
		case err := <-errCh1:
			require.Error(t, err)
			db1.Exec(t, "SELECT pg_advisory_unlock_all()")
			require.NoError(t, <-errCh2)
		case err := <-errCh2:
			require.Error(t, err)
			db2.Exec(t, "SELECT pg_advisory_unlock_all()")
			require.NoError(t, <-errCh1)
		case <-time.After(30 * time.Second):
			t.Fatal("Timed out waiting for deadlock detection")
		}

		// Release everything.
		db1.Exec(t, "SELECT pg_advisory_unlock_all()")
		db2.Exec(t, "SELECT pg_advisory_unlock_all()")

		// Both sessions should be able to re-acquire the same locks.
		db1.Exec(t, "SELECT pg_advisory_lock(800)")
		db2.Exec(t, "SELECT pg_advisory_lock(801)")

		// Release and re-acquire in opposite order (sequential, no deadlock).
		db1.Exec(t, "SELECT pg_advisory_unlock_all()")
		db2.Exec(t, "SELECT pg_advisory_unlock_all()")
		db1.Exec(t, "SELECT pg_advisory_lock(801)")
		db2.Exec(t, "SELECT pg_advisory_lock(800)")

		// Cleanup
		db1.Exec(t, "SELECT pg_advisory_unlock_all()")
		db2.Exec(t, "SELECT pg_advisory_unlock_all()")
	})

	t.Run("Savepoint Rollback Releases Transaction Locks", func(t *testing.T) {
		db1.Exec(t, "BEGIN")

		// Acquire a lock before the savepoint.
		db1.Exec(t, "SELECT pg_advisory_xact_lock(900)")

		// Create savepoint.
		db1.Exec(t, "SAVEPOINT sp1")

		// Acquire a lock inside the savepoint.
		db1.Exec(t, "SELECT pg_advisory_xact_lock(901)")

		// Session 2 cannot acquire either lock.
		var acquired bool
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(900)").Scan(&acquired)
		require.False(t, acquired)
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(901)").Scan(&acquired)
		require.False(t, acquired)

		// Rollback to savepoint — lock 901 should be released.
		db1.Exec(t, "ROLLBACK TO SAVEPOINT sp1")

		// Lock 900 (before savepoint) should still be held.
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(900)").Scan(&acquired)
		require.False(t, acquired, "Lock acquired before savepoint should survive rollback")

		// Lock 901 (inside savepoint) should be released.
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(901)").Scan(&acquired)
		require.True(t, acquired, "Lock acquired inside savepoint should be released on rollback")
		db2.Exec(t, "SELECT pg_advisory_unlock(901)")

		db1.Exec(t, "COMMIT")
	})

	t.Run("Re-entrant Lock Acquisition", func(t *testing.T) {
		// Same lock acquired multiple times. Each unlock decrements the
		// refcount. The lock is only released when all refs are gone.
		db1.Exec(t, "SELECT pg_advisory_lock(1000)")
		db1.Exec(t, "SELECT pg_advisory_lock(1000)")
		db1.Exec(t, "SELECT pg_advisory_lock(1000)")

		// Session 2 cannot acquire it.
		var acquired bool
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(1000)").Scan(&acquired)
		require.False(t, acquired)

		// First unlock — still held (2 refs remain).
		var released bool
		db1.QueryRow(t, "SELECT pg_advisory_unlock(1000)").Scan(&released)
		require.True(t, released)
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(1000)").Scan(&acquired)
		require.False(t, acquired, "Lock should still be held after first unlock")

		// Second unlock — still held (1 ref remains).
		db1.QueryRow(t, "SELECT pg_advisory_unlock(1000)").Scan(&released)
		require.True(t, released)
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(1000)").Scan(&acquired)
		require.False(t, acquired, "Lock should still be held after second unlock")

		// Third unlock — now released (0 refs).
		db1.QueryRow(t, "SELECT pg_advisory_unlock(1000)").Scan(&released)
		require.True(t, released)
		db2.QueryRow(t, "SELECT pg_try_advisory_lock(1000)").Scan(&acquired)
		require.True(t, acquired, "Lock should be released after all refs unlocked")

		// Cleanup
		db2.Exec(t, "SELECT pg_advisory_unlock(1000)")
	})
}
