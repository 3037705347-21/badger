/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package cmd

import (
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4"
)

func TestRotateReconcileCmd(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	key := make([]byte, 32)
	rand.Read(key)
	opts := badger.DefaultOptions(dir)
	opts.EncryptionKey = key
	opts.BlockCacheSize = 1 << 20
	opts.IndexCacheSize = 1 << 20

	db, err := badger.Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("foo"), make([]byte, 200))
	}))
	require.NoError(t, db.Close())

	key2 := make([]byte, 32)
	rand.Read(key2)
	fpOld, err := os.CreateTemp("", "*.key")
	require.NoError(t, err)
	_, err = fpOld.Write(key)
	require.NoError(t, err)
	require.NoError(t, fpOld.Close())

	fpNew, err := os.CreateTemp("", "*.key")
	require.NoError(t, err)
	_, err = fpNew.Write(key2)
	require.NoError(t, err)
	require.NoError(t, fpNew.Close())

	sstDir = dir
	vlogDir = dir
	reconcileOldKeyPath = fpOld.Name()
	reconcileNewKeyPath = fpNew.Name()
	require.NoError(t, doRotateReconcile(nil, []string{}))

	// Re-running after completion starts a fresh batch and is idempotent.
	reconcileOldKeyPath = fpNew.Name()
	fp3, err := os.CreateTemp("", "*.key")
	require.NoError(t, err)
	key3 := make([]byte, 32)
	rand.Read(key3)
	_, err = fp3.Write(key3)
	require.NoError(t, err)
	require.NoError(t, fp3.Close())
	reconcileNewKeyPath = fp3.Name()
	require.NoError(t, doRotateReconcile(nil, []string{}))

	opts.EncryptionKey = key3
	db, err = badger.Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Close())
}
