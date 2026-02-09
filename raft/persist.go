package raft

import (
	"fmt"
)

// persist saves Raft log entries to WAL
func (rf *Raft) persist() {
	if rf.wal == nil {
		return
	}

	// Only persist if we have log entries
	if len(rf.log) > 0 {
		// Convert only new entries (WAL.AppendEntries handles deduplication)
		walEntries := make([]WALEntry, 0, len(rf.log))
		for _, entry := range rf.log {
			walEntries = append(walEntries, WALEntry{
				RecordType: RecordTypeLog,
				Index:      uint32(entry.Index),
				Term:       uint32(entry.Term),
				Command:    entry.Command,
			})
		}

		// WAL handles skipping already-persisted entries internally
		rf.wal.AppendEntries(walEntries, 0)
	}
}

// persistState saves Raft hard state (term, vote, commit) to WAL: only called when these values actually change
func (rf *Raft) persistState() {
	if rf.wal == nil {
		return
	}

	rf.wal.PersistHardState(
		uint32(rf.currentTerm),
		uint32(rf.votedFor),
		uint32(rf.commitIndex),
	)
}

// readPersist restores Raft state from the WAL
func (rf *Raft) readPersist() {
	if rf.wal == nil {
		fmt.Printf("WAL not initialized for node %d\n", rf.me)
		return
	}

	walEntries, hardState, err := rf.wal.RecoverEntries()
	if err != nil {
		fmt.Printf("raft readPersist WAL recovery err node %d: %s\n", rf.me, err)
		return
	}

	if len(walEntries) > 0 {
		// Clear existing log completely and rebuild from WAL
		rf.log = make([]LogEntry, 0, len(walEntries))
		
		// Restore log entries from WAL (includes sentinel at index 0)
		for _, walEntry := range walEntries {
			if walEntry.RecordType == RecordTypeLog {
				rf.log = append(rf.log, LogEntry{
					Index:   int(walEntry.Index),
					Term:    int(walEntry.Term),
					Command: walEntry.Command,
				})
			}
		}
	}

	// Restore hard state if available
	if hardState.Term > 0 {
		rf.currentTerm = int(hardState.Term)
		rf.votedFor = int(hardState.Vote)
		rf.commitIndex = int(hardState.Commit)
		fmt.Printf("Node %d recovered hard state: Term=%d, Vote=%d, Commit=%d\n",
			rf.me, rf.currentTerm, rf.votedFor, rf.commitIndex)
	}
}
