package warppipe

// LROption is a LogicalReplicationListener option function
type LROption func(*LogicalReplicationListener)

// ReplSlotName is an option for setting the replication slot name.
func ReplSlotName(name string) LROption {
	return func(l *LogicalReplicationListener) {
		l.replSlotName = name
	}
}

// StartFromLSN is an option for setting the logical sequence number to start from.
func StartFromLSN(lsn uint64) LROption {
	return func(l *LogicalReplicationListener) {
		l.replLSN = lsn
	}
}

// HeartbeatInterval is an option for setting the connection heartbeat interval.
func HeartbeatInterval(seconds int) LROption {
	return func(l *LogicalReplicationListener) {
		l.connHeartbeatIntervalSeconds = seconds
	}
}
