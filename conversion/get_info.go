// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package conversion handles initial setup for the command line tool
// and web APIs.

// TODO:(searce) Organize code in go style format to make this file more readable.
//
//	public constants first
//	key public type definitions next (although often it makes sense to put them next to public functions that use them)
//	then public functions (and relevant type definitions)
//	and helper functions and other non-public definitions last (generally in order of importance)
package conversion

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"

	"cloud.google.com/go/cloudsqlconn"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/cloudsql"
	cloudsqlconnaccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/cloudsqlconn"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/dynamodb"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/mysql"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/oracle"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/postgres"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/sqlserver"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	dydb "github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

type GetInfoInterface interface{
	getInfoSchemaForShard(shardConnInfo profiles.DirectConnectionConfig, driver string, targetProfile profiles.TargetProfile, s profiles.SourceProfileDialectInterface, g GetInfoInterface) (common.InfoSchema, error)
	GetInfoSchemaFromCloudSQL(sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile) (common.InfoSchema, error)
	GetInfoSchema(sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile) (common.InfoSchema, error)	
}
type GetInfoImpl struct{}

func (gi *GetInfoImpl) getInfoSchemaForShard(shardConnInfo profiles.DirectConnectionConfig, driver string, targetProfile profiles.TargetProfile, s profiles.SourceProfileDialectInterface, g GetInfoInterface) (common.InfoSchema, error) {
	params := make(map[string]string)
	params["host"] = shardConnInfo.Host
	params["user"] = shardConnInfo.User
	params["dbName"] = shardConnInfo.DbName
	params["port"] = shardConnInfo.Port
	params["password"] = shardConnInfo.Password
	//while adding other sources, a switch-case will be added here on the basis of the driver input param passed.
	//pased on the driver name, profiles.NewSourceProfileConnection<DBName> will need to be called to create
	//the source profile information.
	getUtilsInfo := utils.GetUtilInfoImpl{}
	sourceProfileConnectionMySQL, err := s.NewSourceProfileConnectionMySQL(params, &getUtilsInfo)
	if err != nil {
		return nil, fmt.Errorf("cannot parse connection configuration for the primary shard")
	}
	sourceProfileConnection := profiles.SourceProfileConnection{Mysql: sourceProfileConnectionMySQL, Ty: profiles.SourceProfileConnectionTypeMySQL}
	//create a source profile which contains the sourceProfileConnection object for the primary shard
	//this is done because GetSQLConnectionStr() should not be aware of sharding
	newSourceProfile := profiles.SourceProfile{Conn: sourceProfileConnection, Ty: profiles.SourceProfileTypeConnection}
	newSourceProfile.Driver = driver
	infoSchema, err := g.GetInfoSchema(newSourceProfile, targetProfile)
	if err != nil {
		return nil, err
	}
	return infoSchema, nil
}


func (gi *GetInfoImpl) GetInfoSchemaFromCloudSQL(sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile) (common.InfoSchema, error) {
	driver := sourceProfile.Driver
	switch driver {
	case constants.MYSQL:
		csc, err := cloudsql.NewCloudSqlConnDialerImpl(context.Background())
		if err != nil {
			return nil, fmt.Errorf("cloudsqlconn.NewDialer: %w", err)
		}
        if err != nil {
                return nil, fmt.Errorf("cloudsqlconn.NewDialer: %w", err)
        }
        var opts []cloudsqlconn.DialOption
		instanceName := fmt.Sprintf("%s:%s:%s", sourceProfile.ConnCloudSQL.Mysql.Project, sourceProfile.ConnCloudSQL.Mysql.Region, sourceProfile.ConnCloudSQL.Mysql.InstanceName)
        mysqldriver.RegisterDialContext("cloudsqlconn",
                func(ctx context.Context, addr string) (net.Conn, error) {
					cscA := cloudsqlconnaccessor.CloudSqlConnAccessorImpl{}
                    return cscA.Dial(context.Background(), csc, instanceName, opts...)
                })

        dbURI := fmt.Sprintf("%s:empty@cloudsqlconn(localhost:3306)/%s?parseTime=true",
                sourceProfile.ConnCloudSQL.Mysql.User, sourceProfile.ConnCloudSQL.Mysql.Db)

        db, err := sql.Open("mysql", dbURI)
		if err != nil {
			return nil, fmt.Errorf("sql.Open: %w", err)
		}
		return mysql.InfoSchemaImpl{
			DbName:        sourceProfile.ConnCloudSQL.Mysql.Db,
			Db:            db,
			SourceProfile: sourceProfile,
			TargetProfile: targetProfile,
		}, nil
	case constants.POSTGRES:
		d, err := cloudsqlconn.NewDialer(context.Background(), cloudsqlconn.WithIAMAuthN())
        if err != nil {
                return nil, fmt.Errorf("cloudsqlconn.NewDialer: %w", err)
        }
        var opts []cloudsqlconn.DialOption

        dsn := fmt.Sprintf("user=%s database=%s", sourceProfile.ConnCloudSQL.Pg.User, sourceProfile.ConnCloudSQL.Pg.Db)
        config, err := pgx.ParseConfig(dsn)
        if err != nil {
                return nil, err
        }
		instanceName := fmt.Sprintf("%s:%s:%s", sourceProfile.ConnCloudSQL.Pg.Project, sourceProfile.ConnCloudSQL.Pg.Region, sourceProfile.ConnCloudSQL.Pg.InstanceName)
        config.DialFunc = func(ctx context.Context, network, instance string) (net.Conn, error) {
                return d.Dial(ctx, instanceName, opts...)
        }
        dbURI := stdlib.RegisterConnConfig(config)
        db, err := sql.Open("pgx", dbURI)
        if err != nil {
                return nil, fmt.Errorf("sql.Open: %w", err)
        }
		temp := false
		return postgres.InfoSchemaImpl{
			Db:             db,
			SourceProfile:  sourceProfile,
			TargetProfile:  targetProfile,
			IsSchemaUnique: &temp, //this is a workaround to set a bool pointer
		}, nil
	default:
		return nil, fmt.Errorf("driver %s not supported", driver)
	}
}


func (gi *GetInfoImpl) GetInfoSchema(sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile) (common.InfoSchema, error) {
	connectionConfig, err := connectionConfig(sourceProfile)
	if err != nil {
		return nil, err
	}
	driver := sourceProfile.Driver
	switch driver {
	case constants.MYSQL:
		db, err := sql.Open(driver, connectionConfig.(string))
		dbName := getDbNameFromSQLConnectionStr(driver, connectionConfig.(string))
		if err != nil {
			return nil, err
		}
		return mysql.InfoSchemaImpl{
			DbName:        dbName,
			Db:            db,
			SourceProfile: sourceProfile,
			TargetProfile: targetProfile,
		}, nil
	case constants.POSTGRES:
		db, err := sql.Open(driver, connectionConfig.(string))
		if err != nil {
			return nil, err
		}
		temp := false
		return postgres.InfoSchemaImpl{
			Db:             db,
			SourceProfile:  sourceProfile,
			TargetProfile:  targetProfile,
			IsSchemaUnique: &temp, //this is a workaround to set a bool pointer
		}, nil
	case constants.DYNAMODB:
		mySession := session.Must(session.NewSession())
		dydbClient := dydb.New(mySession, connectionConfig.(*aws.Config))
		var dydbStreamsClient *dynamodbstreams.DynamoDBStreams
		if sourceProfile.Conn.Streaming {
			newSession := session.Must(session.NewSession())
			dydbStreamsClient = dynamodbstreams.New(newSession, connectionConfig.(*aws.Config))
		}
		return dynamodb.InfoSchemaImpl{
			DynamoClient:        dydbClient,
			SampleSize:          profiles.GetSchemaSampleSize(sourceProfile),
			DynamoStreamsClient: dydbStreamsClient,
		}, nil
	case constants.SQLSERVER:
		db, err := sql.Open(driver, connectionConfig.(string))
		dbName := getDbNameFromSQLConnectionStr(driver, connectionConfig.(string))
		if err != nil {
			return nil, err
		}
		return sqlserver.InfoSchemaImpl{DbName: dbName, Db: db}, nil
	case constants.ORACLE:
		db, err := sql.Open(driver, connectionConfig.(string))
		dbName := getDbNameFromSQLConnectionStr(driver, connectionConfig.(string))
		if err != nil {
			return nil, err
		}
		return oracle.InfoSchemaImpl{DbName: strings.ToUpper(dbName), Db: db, SourceProfile: sourceProfile, TargetProfile: targetProfile}, nil
	default:
		return nil, fmt.Errorf("driver %s not supported", driver)
	}
}