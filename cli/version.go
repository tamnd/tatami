package cli

// Build metadata, stamped via -ldflags at release time. goreleaser targets
// github.com/tamnd/tatami/cli.{Version,Commit,Date}.
var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)
