package warppipe

import "time"

// NotifyOption is a NotifyListener option function
type NotifyOption func(*NotifyListener)

// StartFromID is an option for setting the startFromID
func StartFromID(changesetID int64) NotifyOption {
	return func(l *NotifyListener) {
		l.startFromID = &changesetID
	}
}

// StartFromTimestamp is an option for setting the startFromTimestamp
func StartFromTimestamp(t time.Time) NotifyOption {
	return func(l *NotifyListener) {
		l.startFromTimestamp = &t
	}
}
