package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-pkgz/lgr"
	"github.com/umputun/go-flags"

	"github.com/umputun/sys-agent/app/config"
	"github.com/umputun/sys-agent/app/server"
	"github.com/umputun/sys-agent/app/status"
	"github.com/umputun/sys-agent/app/status/external"
)

var revision string

var opts struct {
	Config string `short:"f" long:"config" env:"CONFIG" description:"config file"`

	Listen  string   `short:"l" long:"listen" env:"LISTEN" default:"localhost:8080" description:"listen on host:port"`
	Volumes []string `short:"v" long:"volume" env:"VOLUMES" default:"root:/" env-delim:"," description:"volumes to report"`

	Services []string      `short:"s" long:"service" env:"SERVICES" env-delim:"," description:"services to report"`
	TimeOut  time.Duration `long:"timeout" env:"TIMEOUT" default:"5s" description:"timeout for each request to services"`

	Concurrency int  `long:"concurrency" env:"CONCURRENCY" default:"4" description:"number of concurrent requests to services"`
	Dbg         bool `long:"dbg" env:"DEBUG" description:"show debug info"`
}

func main() {
	fmt.Printf("sys-agent %s\n", revision)

	p := flags.NewParser(&opts, flags.PassDoubleDash|flags.HelpFlag)
	if _, err := p.Parse(); err != nil {
		if err.(*flags.Error).Type != flags.ErrHelp {
			fmt.Printf("%v\n", err)
			os.Exit(1)
		}
		p.WriteHelp(os.Stderr)
		os.Exit(2)
	}
	setupLog(opts.Dbg)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if x := recover(); x != nil {
			log.Printf("[WARN] run time panic:\n%v", x)
			panic(x)
		}

		// catch signal and invoke graceful termination
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
		<-stop
		log.Printf("[WARN] interrupt signal")
		cancel()
	}()

	var conf *config.Parameters
	if opts.Config != "" {
		var err error
		conf, err = config.New(opts.Config)
		if err != nil {
			log.Fatalf("[ERROR] can't load config, %s", err)
		}
		log.Printf("[DEBUG] %s", conf.String())
	}

	vols, err := parseVolumes(opts.Volumes, conf)
	if err != nil {
		log.Fatalf("[ERROR] %s", err)
	}

	providers := external.Providers{
		HTTP:        &external.HTTPProvider{Client: http.Client{Timeout: opts.TimeOut}},
		Mongo:       &external.MongoProvider{TimeOut: opts.TimeOut},
		Docker:      &external.DockerProvider{TimeOut: opts.TimeOut},
		Program:     &external.ProgramProvider{TimeOut: opts.TimeOut, WithShell: true},
		Nginx:       &external.NginxProvider{TimeOut: opts.TimeOut},
		Certificate: &external.CertificateProvider{TimeOut: opts.TimeOut},
		File:        &external.FileProvider{TimeOut: opts.TimeOut},
		RMQ:         &external.RMQProvider{TimeOut: opts.TimeOut},
		Mysql:       &external.MysqlProvider{TimeOut: opts.TimeOut},
	}

	srv := server.Rest{
		Listen:  opts.Listen,
		Version: revision,
		Status: &status.Service{
			Volumes:     vols,
			ExtServices: external.NewService(providers, opts.Concurrency, services(opts.Services, conf)...),
		},
	}

	if err := srv.Run(ctx); err != nil && err.Error() != "http: Server closed" {
		log.Fatalf("[ERROR] %s", err)
	}
}

// service returns list of services to check, merge config and command line
func services(optsSvcs []string, conf *config.Parameters) (res []string) {
	if len(optsSvcs) > 0 {
		res = optsSvcs
	}
	if conf != nil {
		res = append(res, conf.MarshalServices()...)
	}
	log.Printf("[DEBUG] services: %+v", res)
	return res
}

// parseVolumes parses volumes from string list, each element in format "name:path"
// picks volumes from config if present and overrides with command line
func parseVolumes(volumes []string, conf *config.Parameters) ([]status.Volume, error) {
	res := []status.Volume{}

	// load from config if present and volumes provided
	if conf != nil && len(conf.Volumes) > 0 {
		for _, v := range conf.Volumes {
			res = append(res, status.Volume{Name: v.Name, Path: v.Path})
		}
	}

	// load from command line even if config present
	if len(volumes) > 0 {
		res = []status.Volume{} // reset volumes from config (if filled), don't merge
		for _, v := range volumes {
			parts := strings.SplitN(v, ":", 2)
			if len(parts) != 2 {
				return nil, errors.New("invalid volume format, should be <name>:<path>")
			}
			res = append(res, status.Volume{Name: parts[0], Path: parts[1]})
		}
	}

	log.Printf("[DEBUG] volumes: %+v", res)
	return res, nil
}

func setupLog(dbg bool) {
	logOpts := []lgr.Option{lgr.Msec, lgr.LevelBraces, lgr.StackTraceOnError}
	if dbg {
		logOpts = []lgr.Option{lgr.Debug, lgr.CallerFile, lgr.CallerFunc, lgr.Msec, lgr.LevelBraces, lgr.StackTraceOnError}
	}
	lgr.SetupStdLogger(logOpts...)
	lgr.Setup(logOpts...)
}
