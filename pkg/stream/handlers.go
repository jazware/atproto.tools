package stream

import (
	"cmp"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/labstack/echo/v4"
)

type JSONRecord struct {
	FirehoseSeq int64                  `json:"seq"`
	Repo        string                 `json:"repo"`
	Handle      string                 `json:"handle"`
	PDS         string                 `json:"pds"`
	Collection  string                 `json:"collection"`
	RKey        string                 `json:"rkey"`
	Action      string                 `json:"action"`
	Raw         map[string]interface{} `json:"raw,omitempty"`
}

type RecordsResponse struct {
	Records []JSONRecord `json:"records"`
	Error   string       `json:"error,omitempty"`
}

type RecordsQuery struct {
	DID        *syntax.DID
	Collection *syntax.NSID
	Rkey       *syntax.RecordKey
	Seq        *int64
	Limit      int
}

func dbRecordIDToJSONRecord(r Record, id *Identity) JSONRecord {
	rec := JSONRecord{
		FirehoseSeq: r.FirehoseSeq,
		Repo:        r.Repo,
		Collection:  r.Collection,
		RKey:        r.RKey,
		Action:      r.Action,
	}

	if r.Raw != nil {
		// Convert the RAW field to a JSON object
		var rawAsJSON map[string]interface{}
		err := json.Unmarshal(r.Raw, &rawAsJSON)
		if err != nil {
			rawAsJSON = map[string]interface{}{"error": err.Error()}
		}
		rec.Raw = rawAsJSON
	}

	if id != nil {
		pdsURL, err := url.Parse(id.PDS)
		if err != nil {
			pdsURL = &url.URL{}
		}
		rec.Handle = id.Handle
		rec.PDS = pdsURL.Host
	}

	return rec
}

// HandleGetRecords handles the GET /records endpoint
func (s *Stream) HandleGetRecords(c echo.Context) error {
	// Parse the query parameters
	// did - Repo DID (optional)
	// collection - Collection NSID (optional)
	// rkey - Record Key (optional)
	// seq - Firehose sequence number (optional)
	// limit - Number of records to return (default=100)

	// Validate the query parameters
	didParam := c.QueryParam("did")
	collectionParam := c.QueryParam("collection")
	rkeyParam := c.QueryParam("rkey")
	seqParam := c.QueryParam("seq")
	limitParam := c.QueryParam("limit")

	resp := RecordsResponse{}

	query := RecordsQuery{}

	if didParam != "" {
		did, err := syntax.ParseDID(didParam)
		if err != nil {
			resp.Error = fmt.Sprintf("invalid DID: %s", err)
			return c.JSON(http.StatusBadRequest, resp)
		}
		query.DID = &did
	}

	if collectionParam != "" {
		collection, err := syntax.ParseNSID(collectionParam)
		if err != nil {
			resp.Error = fmt.Sprintf("invalid collection: %s", err)
			return c.JSON(http.StatusBadRequest, resp)
		}
		query.Collection = &collection
	}

	if rkeyParam != "" {
		rkey, err := syntax.ParseRecordKey(rkeyParam)
		if err != nil {
			resp.Error = fmt.Sprintf("invalid record key: %s", err)
			return c.JSON(http.StatusBadRequest, resp)
		}
		query.Rkey = &rkey
	}

	if seqParam != "" {
		seq, err := strconv.ParseInt(seqParam, 10, 64)
		if err != nil {
			resp.Error = fmt.Sprintf("invalid sequence number: %s", err)
			return c.JSON(http.StatusBadRequest, resp)
		}
		query.Seq = &seq
	}

	// Only allow querying by collection if a DID is provided
	// Only allow querying by rkey if a DID and collection are provided
	if query.Collection != nil && query.DID == nil {
		resp.Error = "cannot query by collection without a DID"
		return c.JSON(http.StatusBadRequest, resp)
	}
	if query.Rkey != nil && (query.DID == nil || query.Collection == nil) {
		resp.Error = "cannot query by rkey without a DID and collection"
		return c.JSON(http.StatusBadRequest, resp)
	}

	if limitParam != "" {
		limit, err := strconv.Atoi(limitParam)
		if err != nil {
			resp.Error = fmt.Sprintf("invalid limit: %s", err)
			return c.JSON(http.StatusBadRequest, resp)
		}
		query.Limit = limit
	} else {
		query.Limit = 100
	}

	if query.Limit < 1 {
		query.Limit = 100
	}

	if query.Limit > 1000 {
		query.Limit = 1000
	}

	// Build query
	sqlQuery := "SELECT id, firehose_seq, repo, collection, r_key, action, raw, created_at FROM records WHERE 1=1"
	args := []interface{}{}

	if query.DID != nil {
		sqlQuery += " AND repo = ?"
		args = append(args, query.DID.String())
	}
	if query.Collection != nil {
		sqlQuery += " AND collection = ?"
		args = append(args, query.Collection.String())
	}
	if query.Rkey != nil {
		sqlQuery += " AND r_key = ?"
		args = append(args, query.Rkey.String())
	}
	if query.Seq != nil {
		sqlQuery += " AND firehose_seq = ?"
		args = append(args, *query.Seq)
	}

	sqlQuery += " ORDER BY id DESC LIMIT ?"
	args = append(args, query.Limit)

	// Query the database
	rows, err := s.db.Query(sqlQuery, args...)
	if err != nil {
		resp.Error = err.Error()
		return c.JSON(http.StatusInternalServerError, resp)
	}
	defer rows.Close()

	var records []Record
	for rows.Next() {
		var r Record
		var rawJSON sql.NullString
		err := rows.Scan(&r.ID, &r.FirehoseSeq, &r.Repo, &r.Collection, &r.RKey, &r.Action, &rawJSON, &r.CreatedAt)
		if err != nil {
			resp.Error = err.Error()
			return c.JSON(http.StatusInternalServerError, resp)
		}
		if rawJSON.Valid {
			r.Raw = []byte(rawJSON.String)
		}
		records = append(records, r)
	}

	// Query the database for identities
	var identities []Identity
	var dids []string
	for _, r := range records {
		dids = append(dids, r.Repo)
	}

	if len(dids) > 0 {
		// Build IN clause
		placeholders := ""
		idArgs := []interface{}{}
		for i, did := range dids {
			if i > 0 {
				placeholders += ", "
			}
			placeholders += "?"
			idArgs = append(idArgs, did)
		}

		idRows, err := s.db.Query("SELECT d_id, handle, pds, created_at, updated_at FROM identities WHERE d_id IN ("+placeholders+")", idArgs...)
		if err != nil {
			resp.Error = err.Error()
			return c.JSON(http.StatusInternalServerError, resp)
		}
		defer idRows.Close()

		for idRows.Next() {
			var id Identity
			err := idRows.Scan(&id.DID, &id.Handle, &id.PDS, &id.CreatedAt, &id.UpdatedAt)
			if err != nil {
				resp.Error = err.Error()
				return c.JSON(http.StatusInternalServerError, resp)
			}
			identities = append(identities, id)
		}
	}

	// Convert the identities to a map
	identityMap := make(map[string]*Identity)
	for i := range identities {
		id := identities[i]
		identityMap[id.DID] = &id
	}

	// Convert the records to JSON
	resp.Records = make([]JSONRecord, len(records))
	for i, r := range records {
		resp.Records[i] = dbRecordIDToJSONRecord(r, identityMap[r.Repo])
	}

	// Do a final sort by firehose sequence number
	slices.SortFunc(resp.Records, recordSeqSortFunc)

	return c.JSON(http.StatusOK, resp)
}

type JSONEvent struct {
	FirehoseSeq int64   `json:"seq"`
	Repo        string  `json:"repo"`
	EventType   string  `json:"event_type"`
	Error       string  `json:"error,omitempty"`
	Time        int64   `json:"time"`
	Since       *string `json:"since"`
}

type EventsResponse struct {
	Events []JSONEvent `json:"events"`
	Error  string      `json:"error,omitempty"`
}

type EventsQuery struct {
	DID       *syntax.DID
	EventType *string
	Seq       *int64
	Limit     int
}

func dbEventToJSONEvent(e Event) JSONEvent {
	return JSONEvent{
		FirehoseSeq: e.FirehoseSeq,
		Repo:        e.Repo,
		EventType:   e.EventType,
		Error:       e.Error,
		Time:        e.Time,
		Since:       e.Since,
	}
}

// HandleGetEvents handles the GET /events endpoint
func (s *Stream) HandleGetEvents(c echo.Context) error {
	// Parse the query parameters
	// did - Repo DID (optional)
	// event_type - Event type (optional)
	// seq - Firehose sequence number (optional)
	// limit - Number of events to return (default=100)

	// Validate the query parameters
	didParam := c.QueryParam("did")
	eventTypeParam := c.QueryParam("event_type")
	seqParam := c.QueryParam("seq")
	limitParam := c.QueryParam("limit")

	resp := EventsResponse{}

	query := EventsQuery{}

	if didParam != "" {
		did, err := syntax.ParseDID(didParam)
		if err != nil {
			resp.Error = fmt.Sprintf("invalid DID: %s", err)
			return c.JSON(http.StatusBadRequest, resp)
		}
		query.DID = &did
	}

	if eventTypeParam != "" {
		query.EventType = &eventTypeParam
	}

	if seqParam != "" {
		seq, err := strconv.ParseInt(seqParam, 10, 64)
		if err != nil {
			resp.Error = fmt.Sprintf("invalid sequence number: %s", err)
			return c.JSON(http.StatusBadRequest, resp)
		}
		query.Seq = &seq
	}

	if limitParam != "" {
		limit, err := strconv.Atoi(limitParam)
		if err != nil {
			resp.Error = fmt.Sprintf("invalid limit: %s", err)
			return c.JSON(http.StatusBadRequest, resp)
		}
		query.Limit = limit
	} else {
		query.Limit = 100
	}

	if query.Limit < 1 {
		query.Limit = 100
	}

	if query.Limit > 1000 {
		query.Limit = 1000
	}

	// Build query
	sqlQuery := "SELECT firehose_seq, repo, event_type, error, time, since, created_at FROM events WHERE 1=1"
	args := []interface{}{}

	if query.DID != nil {
		sqlQuery += " AND repo = ?"
		args = append(args, query.DID.String())
	}
	if query.EventType != nil {
		sqlQuery += " AND event_type = ?"
		args = append(args, *query.EventType)
	}
	if query.Seq != nil {
		sqlQuery += " AND firehose_seq = ?"
		args = append(args, *query.Seq)
	}

	sqlQuery += " ORDER BY firehose_seq DESC LIMIT ?"
	args = append(args, query.Limit)

	// Query the database
	rows, err := s.db.Query(sqlQuery, args...)
	if err != nil {
		resp.Error = err.Error()
		return c.JSON(http.StatusInternalServerError, resp)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		var e Event
		var since sql.NullString
		err := rows.Scan(&e.FirehoseSeq, &e.Repo, &e.EventType, &e.Error, &e.Time, &since, &e.CreatedAt)
		if err != nil {
			resp.Error = err.Error()
			return c.JSON(http.StatusInternalServerError, resp)
		}
		if since.Valid {
			e.Since = &since.String
		}
		events = append(events, e)
	}

	// Convert the events to JSON
	resp.Events = make([]JSONEvent, len(events))
	for i, e := range events {
		resp.Events[i] = dbEventToJSONEvent(e)
	}
	return c.JSON(http.StatusOK, resp)
}

type JSONIdentity struct {
	DID       string    `json:"did"`
	Handle    string    `json:"handle"`
	PDS       string    `json:"pds"`
	UpdatedAt time.Time `json:"updated_at"`
}

type IdentitiesResponse struct {
	Identities []JSONIdentity `json:"identities"`
	Error      string         `json:"error,omitempty"`
}

type IdentitiesQuery struct {
	DID    *syntax.DID
	Handle *syntax.Handle
	PDS    *string
	Limit  int
}

func dbIdentityToJSONIdentity(i Identity) JSONIdentity {
	return JSONIdentity{
		DID:       i.DID,
		Handle:    i.Handle,
		PDS:       i.PDS,
		UpdatedAt: i.UpdatedAt,
	}
}

func (s *Stream) HandleGetIdentities(c echo.Context) error {
	// Parse the query parameters
	// did - Repo DID (optional)
	// handle - Repo Handle (optional)
	// pds - Rep PDS endpoint (optional)
	// limit - Number of identities to return (default=100)

	// Validate the query parameters
	didParam := c.QueryParam("did")
	handleParam := c.QueryParam("handle")
	pdsParam := c.QueryParam("pds")
	limitParam := c.QueryParam("limit")

	resp := IdentitiesResponse{}

	query := IdentitiesQuery{}

	if didParam != "" {
		did, err := syntax.ParseDID(didParam)
		if err != nil {
			resp.Error = fmt.Sprintf("invalid DID: %s", err)
			return c.JSON(http.StatusBadRequest, resp)
		}
		query.DID = &did
	}

	if handleParam != "" {
		handle, err := syntax.ParseHandle(handleParam)
		if err != nil {
			resp.Error = fmt.Sprintf("invalid handle: %s", err)
			return c.JSON(http.StatusBadRequest, resp)
		}
		query.Handle = &handle
	}

	if pdsParam != "" {
		query.PDS = &pdsParam
	}

	if limitParam != "" {
		limit, err := strconv.Atoi(limitParam)
		if err != nil {
			resp.Error = fmt.Sprintf("invalid limit: %s", err)
			return c.JSON(http.StatusBadRequest, resp)
		}
		query.Limit = limit
	} else {
		query.Limit = 100
	}

	if query.Limit < 1 {
		query.Limit = 100
	}

	if query.Limit > 1000 {
		query.Limit = 1000
	}

	// Build query
	sqlQuery := "SELECT d_id, handle, pds, created_at, updated_at FROM identities WHERE 1=1"
	args := []interface{}{}

	if query.DID != nil {
		sqlQuery += " AND d_id = ?"
		args = append(args, query.DID.String())
	}
	if query.Handle != nil {
		sqlQuery += " AND handle = ?"
		args = append(args, query.Handle.String())
	}
	if query.PDS != nil {
		sqlQuery += " AND pds = ?"
		args = append(args, *query.PDS)
	}

	sqlQuery += " ORDER BY created_at DESC LIMIT ?"
	args = append(args, query.Limit)

	// Query the database
	rows, err := s.db.Query(sqlQuery, args...)
	if err != nil {
		resp.Error = err.Error()
		return c.JSON(http.StatusInternalServerError, resp)
	}
	defer rows.Close()

	var identities []Identity
	for rows.Next() {
		var id Identity
		err := rows.Scan(&id.DID, &id.Handle, &id.PDS, &id.CreatedAt, &id.UpdatedAt)
		if err != nil {
			resp.Error = err.Error()
			return c.JSON(http.StatusInternalServerError, resp)
		}
		identities = append(identities, id)
	}

	// Convert the identities to JSON
	resp.Identities = make([]JSONIdentity, len(identities))
	for i, id := range identities {
		resp.Identities[i] = dbIdentityToJSONIdentity(id)
	}
	return c.JSON(http.StatusOK, resp)
}

// Sort by firehose sequence number descending
func recordSeqSortFunc(i, j JSONRecord) int {
	return cmp.Compare(j.FirehoseSeq, i.FirehoseSeq)
}
