// Copyright (c) 2026 4evrd contributors
// released under the MIT license

package pebblehistory

import (
	"time"
)

type Config struct {
	// these are intended to be written directly into the config file:
	Enabled bool
	// directory holding the Pebble database
	Path string
	// fsync the WAL on every write; when false, writes survive a process crash
	// but the most recent writes may be lost on an OS crash or power loss
	Sync bool
	// size of the block cache in megabytes (0 for the default)
	CacheSizeMB int `yaml:"cache-size-mb"`

	// XXX these are copied from elsewhere in the config:
	ExpireTime           time.Duration
	TrackAccountMessages bool
}
