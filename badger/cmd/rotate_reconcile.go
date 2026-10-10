/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/dgraph-io/badger/v4"
)

var reconcileOldKeyPath string
var reconcileNewKeyPath string

var rotateReconcileCmd = &cobra.Command{
	Use:   "rotate-reconcile",
	Short: "Rotate master key with a replayable reconciliation journal.",
	Long: "Rotate-reconcile wraps all data keys under a new master key, driven by a " +
		"durable ROTATION journal. If the process is interrupted, re-running the same " +
		"command continues the unfinished batch; already wrapped keys are not wrapped " +
		"again, data keys still referenced by value logs are retained, and any failure " +
		"to persist progress or validate the registry rolls the batch back.",
	RunE: doRotateReconcile,
}

func init() {
	RootCmd.AddCommand(rotateReconcileCmd)
	rotateReconcileCmd.Flags().StringVarP(&reconcileOldKeyPath, "old-key-path", "o",
		"", "Path of the old master key")
	rotateReconcileCmd.Flags().StringVarP(&reconcileNewKeyPath, "new-key-path", "n",
		"", "Path of the new master key")
}

func doRotateReconcile(cmd *cobra.Command, args []string) error {
	oldKey, err := getKey(reconcileOldKeyPath)
	if err != nil {
		return err
	}
	newKey, err := getKey(reconcileNewKeyPath)
	if err != nil {
		return err
	}
	report, err := badger.ReconcileKeyRotation(badger.RotationOptions{
		Dir:              sstDir,
		ValueDir:         vlogDir,
		OldEncryptionKey: oldKey,
		NewEncryptionKey: newKey,
	})
	if err != nil {
		return err
	}
	if report.Resumed {
		fmt.Printf("Resumed rotation batch %d.\n", report.RotationID)
	} else {
		fmt.Printf("Started rotation batch %d.\n", report.RotationID)
	}
	fmt.Printf("Data keys (re)wrapped in this pass: %d.\n", report.DataKeysRewrapped)
	fmt.Printf("Data keys still referenced (retained): %v.\n", report.ReferencedDataKeys)
	if report.Completed {
		fmt.Printf("Rotation %d complete. New master key is now in force.\n",
			report.RotationID)
	}
	return nil
}
