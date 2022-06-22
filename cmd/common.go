package cmd

import (
	"context"
	"fmt"
	"time"

	sp "cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/harbourbridge/common/utils"
	"github.com/cloudspannerecosystem/harbourbridge/conversion"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/profiles"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/writer"
	"github.com/cloudspannerecosystem/harbourbridge/streaming"
)

func startDatastream(ctx context.Context, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile) (streaming.StreamingCfg, error) {
	streamingCfg, err := streaming.ReadStreamingConfig(sourceProfile.Conn.Mysql.StreamingConfig, targetProfile.Conn.Sp.Dbname)
	if err != nil {
		return streamingCfg, fmt.Errorf("error reading streaming config: %v", err)
	}

	err = streaming.LaunchStream(ctx, sourceProfile, targetProfile.Conn.Sp.Project, streamingCfg.DatastreamCfg)
	if err != nil {
		return streamingCfg, fmt.Errorf("error launching stream: %v", err)
	}
	return streamingCfg, nil
}

func performSnapshotMigration(ctx context.Context, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, ioHelper utils.IOStreams, client *sp.Client, conv *internal.Conv, writeLimit int64, dbURI string) (*writer.BatchWriter, error) {
	dataCoversionStartTime := time.Now()
	bw, err := conversion.DataConv(ctx, sourceProfile, targetProfile, &ioHelper, client, conv, true, writeLimit)
	if err != nil {
		return bw, fmt.Errorf("can't finish data conversion for db %s: %v", dbURI, err)
	}
	dataCoversionEndTime := time.Now()
	dataCoversionDuration := dataCoversionEndTime.Sub(dataCoversionStartTime)
	conv.Audit.DataConversionDuration = dataCoversionDuration
	return bw, nil
}

func startDataflow(ctx context.Context, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, streamingCfg streaming.StreamingCfg) error {
	err := streaming.LaunchDataflowJob(ctx, targetProfile, streamingCfg.DatastreamCfg, streamingCfg.DataflowCfg)
	if err != nil {
		return fmt.Errorf("error launching dataflow: %v", err)
	}
	return nil
}

func migrateData(ctx context.Context, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, ioHelper utils.IOStreams, client *sp.Client, conv *internal.Conv, writeLimit int64, dbURI string) (*writer.BatchWriter, error) {
	streamingCfg := streaming.StreamingCfg{}
	var err error
	if sourceProfile.Ty == profiles.SourceProfileTypeConnection && sourceProfile.Conn.Streaming {
		streamingCfg, err = startDatastream(ctx, sourceProfile, targetProfile)
		if err != nil {
			err = fmt.Errorf("error starting datastream: %v", err)
			return nil, err
		}
	}
	bw, err := performSnapshotMigration(ctx, sourceProfile, targetProfile, ioHelper, client, conv, writeLimit, dbURI)
	if err != nil {
		err = fmt.Errorf("can't do snapshot migration: %v", err)
		return nil, err
	}
	if sourceProfile.Ty == profiles.SourceProfileTypeConnection && sourceProfile.Conn.Streaming {
		err = startDataflow(ctx, sourceProfile, targetProfile, streamingCfg)
		if err != nil {
			err = fmt.Errorf("error starting dataflow: %v", err)
			return nil, err
		}
	}
	return bw, nil
}
