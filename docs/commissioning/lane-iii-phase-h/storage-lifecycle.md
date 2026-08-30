# Storage lifecycle

The active L3H event tail remains on the configured hot root. Closure and
archive workers may copy only sealed segments with a checksum and record-count
manifest; they never delete active evidence. Archive-drive loss is degraded
archival, not permission to discard evidence or weaken admission gates.
