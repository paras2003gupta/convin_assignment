-- Make event_id deduplication atomic at the database level.
-- The previous non-unique index allowed concurrent duplicate inserts to race past
-- the application-level EventExists check (TOCTOU). A UNIQUE constraint causes
-- the database engine to reject the second insert atomically, which is then
-- handled by ON CONFLICT DO NOTHING in InsertEvent.
ALTER TABLE events ADD CONSTRAINT events_event_id_key UNIQUE (event_id);

-- The old non-unique index is now superseded by the implicit unique index
-- created above, so drop it to avoid redundant index maintenance.
DROP INDEX IF EXISTS idx_events_event_id;
