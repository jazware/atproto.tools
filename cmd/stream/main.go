package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/ericvolp12/atproto.tools/pkg/stream"
	"github.com/ericvolp12/bsky-experiments/pkg/tracing"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	slogecho "github.com/samber/slog-echo"
	echopprof "github.com/sevenNt/echo-pprof"
	"go.opentelemetry.io/otel"

	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name:    "stream",
		Usage:   "atproto firehose stream consumer",
		Version: "0.0.1",
	}

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "ws-url",
			Usage:   "full websocket path to the ATProto SubscribeRepos XRPC endpoint",
			Value:   "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos",
			EnvVars: []string{"LG_WS_URL"},
		},
		&cli.IntFlag{
			Name:    "port",
			Usage:   "port to serve the http server on",
			Value:   8080,
			EnvVars: []string{"LG_PORT"},
		},
		&cli.BoolFlag{
			Name:    "debug",
			Usage:   "enable debug logging",
			Value:   false,
			EnvVars: []string{"LG_DEBUG"},
		},
		&cli.StringFlag{
			Name:    "duckdb-path",
			Usage:   "path to the DuckDB database",
			Value:   "/data/looking-glass.db",
			EnvVars: []string{"LG_DUCKDB_PATH"},
		},
		&cli.BoolFlag{
			Name:    "migrate-db",
			Usage:   "run database migrations",
			Value:   true,
			EnvVars: []string{"LG_MIGRATE_DB"},
		},
		&cli.DurationFlag{
			Name:    "evt-record-ttl",
			Usage:   "time to live for events and records in the DB",
			Value:   72 * time.Hour,
			EnvVars: []string{"LG_EVT_RECORD_TTL"},
		},
		&cli.Int64Flag{
			Name:    "plc-rate-limit",
			Usage:   "rate limit for PLC lookups in requests per second",
			Value:   100,
			EnvVars: []string{"LG_PLC_RATE_LIMIT"},
		},
		&cli.BoolFlag{
			Name:    "lookup-on-commit",
			Usage:   "lookup DID docs on commit events (don't use for high volume services without ratelimit bypasses)",
			Value:   false,
			EnvVars: []string{"LG_LOOKUP_ON_COMMIT"},
		},
	}

	app.Action = LookingGlass

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

var tracer = otel.Tracer("LookingGlass")

// LookingGlass is the main function for the stream consumer
func LookingGlass(cctx *cli.Context) error {
	ctx, cancel := context.WithCancel(cctx.Context)
	defer cancel()

	// Create a channel that will be closed when we want to stop the application
	// Usually when a critical routine returns an error
	kill := make(chan struct{})

	// Logging
	logLevel := slog.LevelInfo
	if cctx.Bool("debug") {
		logLevel = slog.LevelDebug
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel, AddSource: true}))
	slog.SetDefault(slog.New(logger.Handler()))

	logger.Info("starting up")

	// Registers a tracer Provider globally if the exporter endpoint is set
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		logger.Info("registering global tracer provider")
		shutdown, err := tracing.InstallExportPipeline(ctx, "atp-looking-glass", 1)
		if err != nil {
			logger.Error("failed to install export pipeline", "error", err)
			return err
		}
		defer func() {
			if err := shutdown(ctx); err != nil {
				logger.Error("failed to shutdown export pipeline", "error", err)
			}
		}()
	}

	s, err := stream.NewStream(
		logger,
		cctx.String("ws-url"),
		cctx.String("duckdb-path"),
		cctx.Bool("migrate-db"),
		cctx.Duration("evt-record-ttl"),
		cctx.Int64("plc-rate-limit"),
		cctx.Bool("lookup-on-commit"),
	)
	if err != nil {
		logger.Error("failed to create stream", "error", err)
		return err
	}

	// Start a goroutine to manage the liveness checker, shutting down if no events are received for 15 seconds
	shutdownLivenessChecker := make(chan struct{})
	livenessCheckerShutdown := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		lastSeq := int64(0)

		logger := slog.With("source", "liveness_checker")

		for {
			select {
			case <-shutdownLivenessChecker:
				logger.Info("shutting down liveness checker")
				close(livenessCheckerShutdown)
				return
			case <-ticker.C:
				seq := s.GetSeq()
				cleaningUp := s.GetCleaningUp()
				if seq == lastSeq && !cleaningUp {
					logger.Error("no new events in last 30 seconds, shutting down for docker to restart me", "last_seq", lastSeq)
					close(kill)
				} else if cleaningUp {
					logger.Info("checked for liveness during cleanup, skipping this cycle", "last_seq", lastSeq)
				} else {
					logger.Debug("received new event, resetting liveness timer", "last_seq", seq)
					lastSeq = seq
				}
			}
		}
	}()

	e := echo.New()
	e.HideBanner = true
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowHeaders: []string{echo.HeaderOrigin, echo.HeaderContentType, echo.HeaderAccept, echo.HeaderAuthorization},
	}))
	e.Use(slogecho.New(logger))
	e.Use(stream.MetricsMiddleware)
	e.Use(middleware.Recover())

	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
	e.GET("/records", s.HandleGetRecords)
	e.GET("/events", s.HandleGetEvents)
	e.GET("/identities", s.HandleGetIdentities)
	e.GET("/", func(c echo.Context) error {
		return c.String(http.StatusOK, "Looking Glass")
	})
	echopprof.Wrap(e)

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cctx.Int("port")),
		Handler: e,
	}

	// Startup HTTP server
	shutdownHTTPServer := make(chan struct{})
	httpServerShutdown := make(chan struct{})
	go func() {
		logger := logger.With("source", "http_server")

		logger.Info("http server listening on port", "port", cctx.Int("port"))

		go func() {
			if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
				logger.Error("failed to start http server", "error", err)
			}
		}()
		<-shutdownHTTPServer
		if err := httpServer.Shutdown(ctx); err != nil {
			logger.Error("failed to shut down http server", "error", err)
		}
		logger.Info("http server shut down")
		close(httpServerShutdown)
	}()

	// Run the stream in a goroutine
	streamKill := make(chan struct{})
	streamShutdownFinished := make(chan struct{})
	go func() {
		logger := logger.With("source", "stream")

		logger.Info("starting stream")
		err := s.Start(ctx)
		if err != nil {
			logger.Error("stream returned an error", "error", err)
			close(streamKill)
		}
		logger.Info("stream shut down")
		close(streamShutdownFinished)
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-signals:
		logger.Info("received signal, shutting down")
	case <-ctx.Done():
		logger.Info("context cancelled, shutting down")
	case <-kill:
		logger.Info("shutting down due to liveness checker")
	case <-streamKill:
		logger.Info("shutting down due to stream error")
	}

	logger.Info("shutting down, waiting for routines to finish")
	cancel()
	close(shutdownLivenessChecker)
	close(shutdownHTTPServer)

	<-livenessCheckerShutdown
	<-httpServerShutdown
	<-streamShutdownFinished
	logger.Info("shutdown complete")

	return nil
}
