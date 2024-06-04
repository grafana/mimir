package sqlds

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/data/sqlutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/resource/httpadapter"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

const defaultKeySuffix = "default"

var (
	ErrorMissingMultipleConnectionsConfig = PluginError(errors.New("received connection arguments but the feature is not enabled"))
	ErrorMissingDBConnection              = PluginError(errors.New("unable to get default db connection"))
	HeaderKey                             = "grafana-http-headers"
	// Deprecated: ErrorMissingMultipleConnectionsConfig should be used instead
	MissingMultipleConnectionsConfig = ErrorMissingMultipleConnectionsConfig
	// Deprecated: ErrorMissingDBConnection should be used instead
	MissingDBConnection = ErrorMissingDBConnection
)

func defaultKey(datasourceUID string) string {
	return fmt.Sprintf("%s-%s", datasourceUID, defaultKeySuffix)
}

func keyWithConnectionArgs(datasourceUID string, connArgs json.RawMessage) string {
	return fmt.Sprintf("%s-%s", datasourceUID, string(connArgs))
}

type dbConnection struct {
	db       *sql.DB
	settings backend.DataSourceInstanceSettings
}

type SQLDatasource struct {
	Completable

	dbConnections  sync.Map
	c              Driver
	driverSettings DriverSettings

	backend.CallResourceHandler
	CustomRoutes map[string]func(http.ResponseWriter, *http.Request)
	// Enabling multiple connections may cause that concurrent connection limits
	// are hit. The datasource enabling this should make sure connections are cached
	// if necessary.
	EnableMultipleConnections bool
}

func (ds *SQLDatasource) getDBConnection(key string) (dbConnection, bool) {
	conn, ok := ds.dbConnections.Load(key)
	if !ok {
		return dbConnection{}, false
	}
	return conn.(dbConnection), true
}

func (ds *SQLDatasource) storeDBConnection(key string, dbConn dbConnection) {
	ds.dbConnections.Store(key, dbConn)
}

func getDatasourceUID(settings backend.DataSourceInstanceSettings) string {
	datasourceUID := settings.UID
	// Grafana < 8.0 won't include the UID yet
	if datasourceUID == "" {
		datasourceUID = fmt.Sprintf("%d", settings.ID)
	}
	return datasourceUID
}

// NewDatasource creates a new `SQLDatasource`.
// It uses the provided settings argument to call the ds.Driver to connect to the SQL server
func (ds *SQLDatasource) NewDatasource(ctx context.Context, settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	db, err := ds.c.Connect(ctx, settings, nil)
	if err != nil {
		return nil, DownstreamError(err)
	}
	key := defaultKey(getDatasourceUID(settings))
	ds.storeDBConnection(key, dbConnection{db, settings})

	mux := http.NewServeMux()
	err = ds.registerRoutes(mux)
	if err != nil {
		return nil, PluginError(err)
	}

	ds.CallResourceHandler = httpadapter.New(mux)
	ds.driverSettings = ds.c.Settings(ctx, settings)

	return ds, nil
}

// NewDatasource initializes the Datasource wrapper and instance manager
func NewDatasource(c Driver) *SQLDatasource {
	return &SQLDatasource{
		c: c,
	}
}

// Dispose cleans up datasource instance resources.
// Note: Called when testing and saving a datasource
func (ds *SQLDatasource) Dispose() {
}

// QueryData creates the Responses list and executes each query
func (ds *SQLDatasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	headers := req.GetHTTPHeaders()

	var (
		response = NewResponse(backend.NewQueryDataResponse())
		wg       = sync.WaitGroup{}
	)

	wg.Add(len(req.Queries))

	// Execute each query and store the results by query RefID
	for _, q := range req.Queries {
		go func(query backend.DataQuery) {
			frames, err := ds.handleQuery(ctx, query, getDatasourceUID(*req.PluginContext.DataSourceInstanceSettings), headers)
			if err == nil {
				if responseMutator, ok := ds.c.(ResponseMutator); ok {
					frames, err = responseMutator.MutateResponse(ctx, frames)
					if err != nil {
						err = PluginError(err)
					}
				}
			}

			response.Set(query.RefID, backend.DataResponse{
				Frames:      frames,
				Error:       err,
				ErrorSource: ErrorSource(err),
			})

			wg.Done()
		}(q)
	}

	wg.Wait()

	errs := ds.errors(response)
	if ds.driverSettings.Errors {
		return response.Response(), errs
	}

	return response.Response(), nil
}

func (ds *SQLDatasource) GetDBFromQuery(ctx context.Context, q *Query, datasourceUID string) (*sql.DB, error) {
	_, dbConn, err := ds.getDBConnectionFromQuery(ctx, q, datasourceUID)
	return dbConn.db, err
}

func (ds *SQLDatasource) getDBConnectionFromQuery(ctx context.Context, q *Query, datasourceUID string) (string, dbConnection, error) {
	if !ds.EnableMultipleConnections && !ds.driverSettings.ForwardHeaders && len(q.ConnectionArgs) > 0 {
		return "", dbConnection{}, MissingMultipleConnectionsConfig
	}
	// The database connection may vary depending on query arguments
	// The raw arguments are used as key to store the db connection in memory so they can be reused
	key := defaultKey(datasourceUID)
	dbConn, ok := ds.getDBConnection(key)
	if !ok {
		return "", dbConnection{}, MissingDBConnection
	}
	if !ds.EnableMultipleConnections || len(q.ConnectionArgs) == 0 {
		return key, dbConn, nil
	}

	key = keyWithConnectionArgs(datasourceUID, q.ConnectionArgs)
	if cachedConn, ok := ds.getDBConnection(key); ok {
		return key, cachedConn, nil
	}

	db, err := ds.c.Connect(ctx, dbConn.settings, q.ConnectionArgs)
	if err != nil {
		return "", dbConnection{}, DownstreamError(err)
	}
	// Assign this connection in the cache
	dbConn = dbConnection{db, dbConn.settings}
	ds.storeDBConnection(key, dbConn)

	return key, dbConn, nil
}

// handleQuery will call query, and attempt to reconnect if the query failed
func (ds *SQLDatasource) handleQuery(ctx context.Context, req backend.DataQuery, datasourceUID string, headers http.Header) (data.Frames, error) {
	if queryMutator, ok := ds.c.(QueryMutator); ok {
		ctx, req = queryMutator.MutateQuery(ctx, req)
	}

	// Convert the backend.DataQuery into a Query object
	q, err := GetQuery(req, headers, ds.driverSettings.ForwardHeaders)
	if err != nil {
		return nil, err
	}

	// Apply supported macros to the query
	q.RawSQL, err = Interpolate(ds.c, q)
	if err != nil {
		return sqlutil.ErrorFrameFromQuery(q), fmt.Errorf("%s: %w", "Could not apply macros", err)
	}

	// Apply the default FillMode, overwritting it if the query specifies it
	fillMode := ds.driverSettings.FillMode
	if q.FillMissing != nil {
		fillMode = q.FillMissing
	}

	// Retrieve the database connection
	cacheKey, dbConn, err := ds.getDBConnectionFromQuery(ctx, q, datasourceUID)
	if err != nil {
		return sqlutil.ErrorFrameFromQuery(q), err
	}

	if ds.driverSettings.Timeout != 0 {
		tctx, cancel := context.WithTimeout(ctx, ds.driverSettings.Timeout)
		defer cancel()

		ctx = tctx
	}

	var args []interface{}
	if argSetter, ok := ds.c.(QueryArgSetter); ok {
		args = argSetter.SetQueryArgs(ctx, headers)
	}

	// FIXES:
	//  * Some datasources (snowflake) expire connections or have an authentication token that expires if not used in 1 or 4 hours.
	//    Because the datasource driver does not include an option for permanent connections, we retry the connection
	//    if the query fails. NOTE: this does not include some errors like "ErrNoRows"
	res, err := QueryDB(ctx, dbConn.db, ds.c.Converters(), fillMode, q, args...)
	if err == nil {
		return res, nil
	}

	if errors.Is(err, ErrorNoResults) {
		return res, nil
	}

	// If there's a query error that didn't exceed the
	// context deadline retry the query
	if errors.Is(err, ErrorQuery) && !errors.Is(err, context.DeadlineExceeded) {
		// only retry on messages that contain specific errors
		if shouldRetry(ds.driverSettings.RetryOn, err.Error()) {
			for i := 0; i < ds.driverSettings.Retries; i++ {
				backend.Logger.Warn(fmt.Sprintf("query failed: %s. Retrying %d times", err.Error(), i))
				db, err := ds.dbReconnect(ctx, dbConn, q, cacheKey)
				if err != nil {
					return nil, DownstreamError(err)
				}

				if ds.driverSettings.Pause > 0 {
					time.Sleep(time.Duration(ds.driverSettings.Pause * int(time.Second)))
				}
				res, err = QueryDB(ctx, db, ds.c.Converters(), fillMode, q, args...)
				if err == nil {
					return res, err
				}
				if !shouldRetry(ds.driverSettings.RetryOn, err.Error()) {
					return res, err
				}
				backend.Logger.Warn(fmt.Sprintf("Retry failed: %s", err.Error()))
			}
		}
	}

	// allow retries on timeouts
	if errors.Is(err, context.DeadlineExceeded) {
		for i := 0; i < ds.driverSettings.Retries; i++ {
			backend.Logger.Warn(fmt.Sprintf("connection timed out. retrying %d times", i))
			db, err := ds.dbReconnect(ctx, dbConn, q, cacheKey)
			if err != nil {
				continue
			}

			res, err = QueryDB(ctx, db, ds.c.Converters(), fillMode, q, args...)
			if err == nil {
				return res, err
			}
		}
	}

	return nil, err
}

func (ds *SQLDatasource) dbReconnect(ctx context.Context, dbConn dbConnection, q *Query, cacheKey string) (*sql.DB, error) {
	if err := dbConn.db.Close(); err != nil {
		backend.Logger.Warn(fmt.Sprintf("closing existing connection failed: %s", err.Error()))
	}

	db, err := ds.c.Connect(ctx, dbConn.settings, q.ConnectionArgs)
	if err != nil {
		return nil, DownstreamError(err)
	}
	ds.storeDBConnection(cacheKey, dbConnection{db, dbConn.settings})
	return db, nil
}

// CheckHealth pings the connected SQL database
func (ds *SQLDatasource) CheckHealth(ctx context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	key := defaultKey(getDatasourceUID(*req.PluginContext.DataSourceInstanceSettings))
	dbConn, ok := ds.getDBConnection(key)
	if !ok {
		return nil, ErrorMissingDBConnection
	}

	if ds.driverSettings.Retries == 0 {
		return ds.check(dbConn)
	}

	return ds.checkWithRetries(ctx, dbConn, key, req.GetHTTPHeaders())
}

func (ds *SQLDatasource) DriverSettings() DriverSettings {
	return ds.driverSettings
}

func (ds *SQLDatasource) checkWithRetries(ctx context.Context, conn dbConnection, key string, headers http.Header) (*backend.CheckHealthResult, error) {
	var result *backend.CheckHealthResult

	q := &Query{}
	if ds.driverSettings.ForwardHeaders {
		applyHeaders(q, headers)
	}

	for i := 0; i < ds.driverSettings.Retries; i++ {
		db, err := ds.dbReconnect(ctx, conn, q, key)
		if err != nil {
			return nil, err
		}
		c := dbConnection{
			db:       db,
			settings: conn.settings,
		}
		result, err = ds.check(c)
		if err == nil {
			return result, err
		}

		if !shouldRetry(ds.driverSettings.RetryOn, err.Error()) {
			break
		}

		if ds.driverSettings.Pause > 0 {
			time.Sleep(time.Duration(ds.driverSettings.Pause * int(time.Second)))
		}
		backend.Logger.Warn(fmt.Sprintf("connect failed: %s. Retrying %d times", err.Error(), i))
	}

	// TODO: failed health checks don't return an error
	return result, nil
}

func (ds *SQLDatasource) check(conn dbConnection) (*backend.CheckHealthResult, error) {
	if err := ds.ping(conn); err != nil {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: err.Error(),
		}, DownstreamError(err)
	}

	return &backend.CheckHealthResult{
		Status:  backend.HealthStatusOk,
		Message: "Data source is working",
	}, nil
}

func (ds *SQLDatasource) ping(conn dbConnection) error {
	if ds.driverSettings.Timeout == 0 {
		return conn.db.Ping()
	}

	ctx, cancel := context.WithTimeout(context.Background(), ds.driverSettings.Timeout)
	defer cancel()

	return conn.db.PingContext(ctx)
}

func shouldRetry(retryOn []string, err string) bool {
	for _, r := range retryOn {
		if strings.Contains(err, r) {
			return true
		}
	}
	return false
}

func (ds *SQLDatasource) errors(response *Response) error {
	if response == nil {
		return nil
	}
	res := response.Response()
	if res == nil {
		return nil
	}
	var err error
	for _, r := range res.Responses {
		err = errors.Join(err, r.Error)
	}
	if err != nil {
		backend.Logger.Error(err.Error())
	}
	return err
}
