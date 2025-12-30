package stream

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/araddon/dateparse"
	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/data"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/parallel"
	"github.com/bluesky-social/indigo/repo"
	"github.com/gorilla/websocket"
	"github.com/ipfs/go-cid"
	_ "github.com/marcboeker/go-duckdb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/time/rate"
)

type Stream struct {
	logger    *slog.Logger
	socketURL *url.URL

	scheduler events.Scheduler

	lastSeq    int64
	cleaningUp bool
	seqLk      sync.RWMutex

	streamClosed chan struct{}

	db  *sql.DB
	ttl time.Duration

	dir            *identity.CacheDirectory
	lookupOnCommit bool
}

var tracer = otel.Tracer("stream")

func NewStream(
	logger *slog.Logger,
	socketURL string,
	duckdbPath string,
	migrate bool,
	ttl time.Duration,
	plcRateLimit int64,
	lookupOnCommit bool,
) (*Stream, error) {
	// Open DuckDB connection
	db, err := sql.Open("duckdb", duckdbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	if migrate {
		logger.Info("running database migrations")
		err := runMigrations(db)
		if err != nil {
			return nil, fmt.Errorf("failed to run database migrations: %w", err)
		}
		logger.Info("database migrations complete")
	}

	base := identity.BaseDirectory{
		PLCURL: identity.DefaultPLCURL,
		HTTPClient: http.Client{
			Timeout: time.Second * 15,
		},
		PLCLimiter: rate.NewLimiter(rate.Limit(plcRateLimit), 1),
		Resolver: net.Resolver{
			Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				d := net.Dialer{Timeout: time.Second * 5}
				return d.DialContext(ctx, network, address)
			},
		},
		TryAuthoritativeDNS: true,
		// primary Bluesky PDS instance only supports HTTP resolution method
		SkipDNSDomainSuffixes: []string{".bsky.social"},
	}

	dir := identity.NewCacheDirectory(&base, 500_000, time.Hour*6, time.Minute*2, time.Hour*6)

	u, err := url.Parse(socketURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse socket url: %w", err)
	}

	return &Stream{
		logger:         logger,
		socketURL:      u,
		streamClosed:   make(chan struct{}),
		db:             db,
		ttl:            ttl,
		dir:            &dir,
		lookupOnCommit: lookupOnCommit,
	}, nil
}

func runMigrations(db *sql.DB) error {
	migrations := []string{
		`CREATE TABLE IF NOT EXISTS cursors (
			id INTEGER PRIMARY KEY,
			last_seq BIGINT NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS events (
			firehose_seq BIGINT NOT NULL,
			repo VARCHAR NOT NULL,
			event_type VARCHAR NOT NULL,
			error TEXT,
			time BIGINT,
			since VARCHAR,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (firehose_seq, repo)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_events_repo ON events(repo)`,
		`CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type)`,
		`CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at)`,
		`CREATE SEQUENCE IF NOT EXISTS records_id_seq START 1`,
		`CREATE TABLE IF NOT EXISTS records (
			id INTEGER PRIMARY KEY DEFAULT nextval('records_id_seq'),
			firehose_seq BIGINT NOT NULL,
			repo VARCHAR NOT NULL,
			collection VARCHAR NOT NULL,
			r_key VARCHAR NOT NULL,
			action VARCHAR NOT NULL,
			raw JSON,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_records_repo ON records(repo)`,
		`CREATE INDEX IF NOT EXISTS idx_records_collection ON records(collection)`,
		`CREATE INDEX IF NOT EXISTS idx_records_path ON records(repo, collection, r_key)`,
		`CREATE INDEX IF NOT EXISTS idx_records_seq ON records(firehose_seq)`,
		`CREATE INDEX IF NOT EXISTS idx_records_created ON records(created_at)`,
		`CREATE TABLE IF NOT EXISTS identities (
			d_id VARCHAR PRIMARY KEY,
			handle VARCHAR NOT NULL,
			pds VARCHAR NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
	}

	for _, migration := range migrations {
		_, err := db.Exec(migration)
		if err != nil {
			return fmt.Errorf("failed to run migration: %w", err)
		}
	}

	return nil
}

func (s *Stream) Start(ctx context.Context) error {
	// Load or create the cursor
	var lastSeq int64
	err := s.db.QueryRow("SELECT last_seq FROM cursors ORDER BY id DESC LIMIT 1").Scan(&lastSeq)
	if err != nil {
		if err == sql.ErrNoRows {
			// Create initial cursor
			_, err := s.db.Exec("INSERT INTO cursors (id, last_seq) VALUES (1, 0)")
			if err != nil {
				return fmt.Errorf("failed to create cursor: %w", err)
			}
			lastSeq = 0
		} else {
			return fmt.Errorf("failed to load cursor: %w", err)
		}
	}

	// Start a routine to save the cursor every 60 seconds
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		for {
			select {
			case <-s.streamClosed:
				seq := s.GetSeq()
				s.logger.Info("stream closed, saving cursor", "seq", seq)
				_, err := s.db.Exec("UPDATE cursors SET last_seq = ?, updated_at = CURRENT_TIMESTAMP WHERE id = 1", seq)
				if err != nil {
					s.logger.Error("failed to save cursor", "err", err)
				}
				s.logger.Info("cursor saved")
				return
			case <-ticker.C:
				seq := s.GetSeq()
				s.logger.Info("saving cursor", "seq", seq)
				_, err := s.db.Exec("UPDATE cursors SET last_seq = ?, updated_at = CURRENT_TIMESTAMP WHERE id = 1", seq)
				if err != nil {
					s.logger.Error("failed to save cursor", "err", err)
				}
			}
		}
	}()

	// Start a routine to delete old events and records every 5 minutes
	if s.ttl > 0 {
		go func() {
			ticker := time.NewTicker(5 * time.Minute)
			for {
				select {
				case <-s.streamClosed:
					return
				case <-ticker.C:
					s.logger.Info("deleting old events and records")
					s.SetCleaningUp(true)

					cutoff := time.Now().Add(-s.ttl)
					result, err := s.db.Exec("DELETE FROM events WHERE created_at < ?", cutoff)
					if err != nil {
						s.logger.Error("failed to delete old events", "err", err)
					} else {
						eventsDeleted, _ := result.RowsAffected()
						s.logger.Info("deleted old events", "count", eventsDeleted)
					}

					result, err = s.db.Exec("DELETE FROM records WHERE created_at < ?", cutoff)
					if err != nil {
						s.logger.Error("failed to delete old records", "err", err)
					} else {
						recordsDeleted, _ := result.RowsAffected()
						s.logger.Info("deleted old records", "count", recordsDeleted)
					}

					s.SetCleaningUp(false)
				}
			}
		}()
	}

	socketURL := s.socketURL
	if lastSeq != 0 {
		q := socketURL.Query()
		q.Set("seq", fmt.Sprintf("%d", lastSeq))
		socketURL.RawQuery = q.Encode()
	}

	rsc := events.RepoStreamCallbacks{
		RepoCommit:    s.RepoCommit,
		RepoHandle:    s.RepoHandle,
		RepoIdentity:  s.RepoIdentity,
		RepoInfo:      s.RepoInfo,
		RepoMigrate:   s.RepoMigrate,
		RepoTombstone: s.RepoTombstone,
		LabelLabels:   s.LabelLabels,
		LabelInfo:     s.LabelInfo,
		Error:         s.Error,
	}

	d := websocket.DefaultDialer

	s.logger.Info("connecting to relay", "url", socketURL.String())

	con, _, err := d.Dial(socketURL.String(), http.Header{
		"User-Agent": []string{"atp-looking-glass/0.0.1"},
	})

	if err != nil {
		return fmt.Errorf("failed to connect to relay: %w", err)
	}

	scheduler := parallel.NewScheduler(100, 10, con.RemoteAddr().String(), rsc.EventHandler)

	s.scheduler = scheduler

	if err := events.HandleRepoStream(ctx, con, scheduler); err != nil {
		s.logger.Error("repo stream failed", "err", err)
	}

	s.logger.Info("repo stream shut down")

	close(s.streamClosed)

	return nil
}

func (s *Stream) SetSeq(seq int64) {
	s.seqLk.Lock()
	defer s.seqLk.Unlock()
	s.lastSeq = seq
}

func (s *Stream) GetSeq() int64 {
	s.seqLk.RLock()
	defer s.seqLk.RUnlock()
	return s.lastSeq
}

func (s *Stream) SetCleaningUp(val bool) {
	s.seqLk.Lock()
	defer s.seqLk.Unlock()
	s.cleaningUp = val
}

func (s *Stream) GetCleaningUp() bool {
	s.seqLk.RLock()
	defer s.seqLk.RUnlock()
	return s.cleaningUp
}

func (s *Stream) RepoCommit(evt *atproto.SyncSubscribeRepos_Commit) error {
	ctx := context.Background()
	ctx, span := tracer.Start(ctx, "RepoCommit")
	defer span.End()

	logger := s.logger.With("repo", evt.Repo, "seq", evt.Seq)

	span.SetAttributes(
		attribute.String("repo", evt.Repo),
		attribute.Int64("seq", evt.Seq),
	)

	s.SetSeq(evt.Seq)

	// Record metadata about the event
	eventError := ""
	eventTime := int64(0)
	var eventSince *string
	if evt.Since != nil {
		eventSince = evt.Since
	}

	// Defer saving the event at the end
	defer func() {
		_, err := s.db.Exec(`
			INSERT INTO events (firehose_seq, repo, event_type, error, time, since)
			VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT (firehose_seq, repo) DO UPDATE SET
				error = EXCLUDED.error,
				time = EXCLUDED.time,
				since = EXCLUDED.since
		`, evt.Seq, evt.Repo, "commit", eventError, eventTime, eventSince)
		if err != nil {
			s.logger.Error("failed to insert event", "err", err)
		}
	}()

	if evt.TooBig {
		s.logger.Warn("commit too big", "repo", evt.Repo, "seq", evt.Seq)
		eventError = "commit too big"
		return nil
	}

	r, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.Blocks))
	if err != nil {
		s.logger.Error("failed to read event repo", "err", err)
		eventError = fmt.Sprintf("failed to read event repo: %v", err)
		return nil
	}

	t, err := dateparse.ParseAny(evt.Time)
	if err != nil {
		s.logger.Error("failed to parse time", "err", err)
		eventError = fmt.Sprintf("failed to parse time: %v", err)
		return nil
	}

	eventTime = t.UnixNano()

	did, err := syntax.ParseDID(evt.Repo)
	if err != nil {
		s.logger.Error("failed to parse DID", "err", err)
	} else if s.lookupOnCommit {
		id, fromCache, err := s.dir.LookupDIDWithCacheState(ctx, did)
		if err != nil {
			s.logger.Error("failed to lookup DID", "err", err)
		} else if !fromCache {
			_, err := s.db.Exec(`
				INSERT INTO identities (d_id, handle, pds)
				VALUES (?, ?, ?)
				ON CONFLICT (d_id) DO UPDATE SET
					handle = ?,
					pds = ?
			`, id.DID.String(), id.Handle.String(), id.PDSEndpoint(),
				id.Handle.String(), id.PDSEndpoint())
			if err != nil {
				s.logger.Error("failed to save identity", "err", err)
			}
		}
	}

	for _, op := range evt.Ops {
		switch op.Action {
		case "create", "update":
			if op.Cid == nil {
				logger.Warn("op missing cid", "path", op.Path, "action", op.Action)
				eventError += fmt.Sprintf("op missing cid (path: %q)", op.Path)
				continue
			}

			c := (cid.Cid)(*op.Cid)
			cid, rec, err := r.GetRecordBytes(ctx, op.Path)
			if err != nil {
				logger.Error("failed to get record bytes", "err", err)
				eventError += fmt.Sprintf("failed to get record bytes (path: %q): %v", op.Path, err)
				continue
			}

			if c != cid {
				logger.Warn("cid mismatch", "from_event", c, "from_blocks", cid)
				eventError += fmt.Sprintf("cid mismatch (path: %q): from_event %q, from_blocks %q", op.Path, c, cid)
				continue
			}

			if rec == nil {
				logger.Warn("record not found", "cid", c, "path", op.Path)
				eventError += fmt.Sprintf("record not found (nil bytes loaded from event blocks) path: %q", op.Path)
				continue
			}

			asCbor, err := data.UnmarshalCBOR(*rec)
			if err != nil {
				logger.Error("failed to unmarshal record from CBOR", "err", err, "cid", c, "path", op.Path)
				eventError += fmt.Sprintf("failed to unmarshal record from CBOR (path: %q): %v", op.Path, err)
				continue
			}

			recJSON, err := json.Marshal(asCbor)
			if err != nil {
				logger.Error("failed to marshal record to JSON", "err", err)
				eventError += fmt.Sprintf("failed to marshal record to JSON (path: %q): %v", op.Path, err)
				continue
			}

			recRawURI := fmt.Sprintf("at://%s/%s", evt.Repo, op.Path)
			recURI, err := syntax.ParseATURI(recRawURI)
			if err != nil {
				logger.Error("failed to parse record uri", "err", err)
				eventError += fmt.Sprintf("failed to parse record uri (path: %q): %v", op.Path, err)
				continue
			}

			// Insert record into DuckDB
			_, err = s.db.Exec(`
				INSERT INTO records (firehose_seq, repo, collection, r_key, action, raw)
				VALUES (?, ?, ?, ?, ?, ?)
			`, evt.Seq, recURI.Authority().String(), recURI.Collection().String(),
				recURI.RecordKey().String(), op.Action, string(recJSON))
			if err != nil {
				logger.Error("failed to create db record", "err", err)
				eventError += fmt.Sprintf("failed to create db record (path: %q): %v", op.Path, err)
			}

		case "delete":
			recRawURI := fmt.Sprintf("at://%s/%s", evt.Repo, op.Path)
			recURI, err := syntax.ParseATURI(recRawURI)
			if err != nil {
				logger.Error("failed to parse record uri", "err", err)
				eventError += fmt.Sprintf("failed to parse record uri (path: %q): %v", op.Path, err)
				continue
			}

			// Insert delete record into DuckDB
			_, err = s.db.Exec(`
				INSERT INTO records (firehose_seq, repo, collection, r_key, action)
				VALUES (?, ?, ?, ?, ?)
			`, evt.Seq, recURI.Authority().String(), recURI.Collection().String(),
				recURI.RecordKey().String(), op.Action)
			if err != nil {
				logger.Error("failed to create db record", "err", err)
				eventError += fmt.Sprintf("failed to create db record (path: %q): %v", op.Path, err)
			}
		default:
			logger.Warn("unknown action", "action", op.Action)
			eventError += fmt.Sprintf("unknown action (path: %q): %q", op.Path, op.Action)
		}
	}

	return nil
}

func (s *Stream) RepoHandle(handle *atproto.SyncSubscribeRepos_Handle) error {
	ctx := context.Background()
	ctx, span := tracer.Start(ctx, "RepoHandle")
	defer span.End()

	span.SetAttributes(
		attribute.Int64("seq", handle.Seq),
	)

	s.SetSeq(handle.Seq)

	eventError := ""
	eventTime := int64(0)

	did, err := syntax.ParseDID(handle.Did)
	if err != nil {
		s.logger.Error("failed to parse DID", "err", err)
	} else {
		s.dir.Purge(ctx, did.AtIdentifier())
		id, err := s.dir.LookupDID(ctx, did)
		if err != nil {
			s.logger.Error("failed to lookup DID", "err", err)
		} else {
			_, err := s.db.Exec(`
				INSERT INTO identities (d_id, handle, pds)
				VALUES (?, ?, ?)
				ON CONFLICT (d_id) DO UPDATE SET
					handle = ?,
					pds = ?
			`, id.DID.String(), id.Handle.String(), id.PDSEndpoint(),
				id.Handle.String(), id.PDSEndpoint())
			if err != nil {
				s.logger.Error("failed to save identity", "err", err)
			}
		}
	}

	t, err := dateparse.ParseAny(handle.Time)
	if err != nil {
		s.logger.Error("failed to parse time", "err", err)
		eventError = fmt.Sprintf("failed to parse time: %v", err)
	} else {
		eventTime = t.UnixNano()
	}

	// Save event
	_, err = s.db.Exec(`
		INSERT INTO events (firehose_seq, repo, event_type, error, time, since)
		VALUES (?, ?, ?, ?, ?, NULL)
		ON CONFLICT (firehose_seq, repo) DO UPDATE SET
			error = EXCLUDED.error,
			time = EXCLUDED.time
	`, handle.Seq, handle.Did, "handle", eventError, eventTime)
	if err != nil {
		s.logger.Error("failed to insert event", "err", err)
	}

	return nil
}

func (s *Stream) RepoIdentity(id *atproto.SyncSubscribeRepos_Identity) error {
	ctx := context.Background()
	ctx, span := tracer.Start(ctx, "RepoIdentity")
	defer span.End()

	span.SetAttributes(
		attribute.Int64("seq", id.Seq),
	)

	s.SetSeq(id.Seq)

	eventError := ""
	eventTime := int64(0)

	did, err := syntax.ParseDID(id.Did)
	if err != nil {
		s.logger.Error("failed to parse DID", "err", err)
	} else {
		s.dir.Purge(ctx, did.AtIdentifier())
		identity, err := s.dir.LookupDID(ctx, did)
		if err != nil {
			s.logger.Error("failed to lookup DID", "err", err)
		} else {
			_, err := s.db.Exec(`
				INSERT INTO identities (d_id, handle, pds)
				VALUES (?, ?, ?)
				ON CONFLICT (d_id) DO UPDATE SET
					handle = ?,
					pds = ?
			`, identity.DID.String(), identity.Handle.String(), identity.PDSEndpoint(),
				identity.Handle.String(), identity.PDSEndpoint())
			if err != nil {
				s.logger.Error("failed to save identity", "err", err)
			}
		}
	}

	t, err := dateparse.ParseAny(id.Time)
	if err != nil {
		s.logger.Error("failed to parse time", "err", err)
		eventError = fmt.Sprintf("failed to parse time: %v", err)
	} else {
		eventTime = t.UnixNano()
	}

	// Save event
	_, err = s.db.Exec(`
		INSERT INTO events (firehose_seq, repo, event_type, error, time, since)
		VALUES (?, ?, ?, ?, ?, NULL)
		ON CONFLICT (firehose_seq, repo) DO UPDATE SET
			error = EXCLUDED.error,
			time = EXCLUDED.time
	`, id.Seq, id.Did, "identity", eventError, eventTime)
	if err != nil {
		s.logger.Error("failed to insert event", "err", err)
	}

	return nil
}

func (s *Stream) RepoInfo(info *atproto.SyncSubscribeRepos_Info) error {
	ctx := context.Background()
	ctx, span := tracer.Start(ctx, "RepoInfo")
	defer span.End()

	return nil
}

func (s *Stream) RepoMigrate(migrate *atproto.SyncSubscribeRepos_Migrate) error {
	ctx := context.Background()
	ctx, span := tracer.Start(ctx, "RepoMigrate")
	defer span.End()

	span.SetAttributes(
		attribute.Int64("seq", migrate.Seq),
	)

	s.SetSeq(migrate.Seq)

	eventError := ""
	eventTime := int64(0)

	t, err := dateparse.ParseAny(migrate.Time)
	if err != nil {
		s.logger.Error("failed to parse time", "err", err)
		eventError = fmt.Sprintf("failed to parse time: %v", err)
	} else {
		eventTime = t.UnixNano()
	}

	// Save event
	_, err = s.db.Exec(`
		INSERT INTO events (firehose_seq, repo, event_type, error, time, since)
		VALUES (?, ?, ?, ?, ?, NULL)
		ON CONFLICT (firehose_seq, repo) DO UPDATE SET
			error = EXCLUDED.error,
			time = EXCLUDED.time
	`, migrate.Seq, migrate.Did, "migrate", eventError, eventTime)
	if err != nil {
		s.logger.Error("failed to insert event", "err", err)
	}

	return nil
}

func (s *Stream) RepoTombstone(tomb *atproto.SyncSubscribeRepos_Tombstone) error {
	ctx := context.Background()
	ctx, span := tracer.Start(ctx, "RepoTombstone")
	defer span.End()

	span.SetAttributes(
		attribute.Int64("seq", tomb.Seq),
	)

	s.SetSeq(tomb.Seq)

	eventError := ""
	eventTime := int64(0)

	t, err := dateparse.ParseAny(tomb.Time)
	if err != nil {
		s.logger.Error("failed to parse time", "err", err)
		eventError = fmt.Sprintf("failed to parse time: %v", err)
	} else {
		eventTime = t.UnixNano()
	}

	// Save event
	_, err = s.db.Exec(`
		INSERT INTO events (firehose_seq, repo, event_type, error, time, since)
		VALUES (?, ?, ?, ?, ?, NULL)
		ON CONFLICT (firehose_seq, repo) DO UPDATE SET
			error = EXCLUDED.error,
			time = EXCLUDED.time
	`, tomb.Seq, tomb.Did, "tombstone", eventError, eventTime)
	if err != nil {
		s.logger.Error("failed to insert event", "err", err)
	}

	return nil
}

func (s *Stream) LabelLabels(label *atproto.LabelSubscribeLabels_Labels) error {
	ctx := context.Background()
	ctx, span := tracer.Start(ctx, "LabelLabels")
	defer span.End()

	span.SetAttributes(
		attribute.Int64("seq", label.Seq),
	)

	s.SetSeq(label.Seq)

	return nil
}

func (s *Stream) LabelInfo(info *atproto.LabelSubscribeLabels_Info) error {
	ctx := context.Background()
	ctx, span := tracer.Start(ctx, "LabelInfo")
	defer span.End()

	return nil
}

func (s *Stream) Error(err *events.ErrorFrame) error {
	ctx := context.Background()
	ctx, span := tracer.Start(ctx, "Error")
	defer span.End()

	return nil
}
