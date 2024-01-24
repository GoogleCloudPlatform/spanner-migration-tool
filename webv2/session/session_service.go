package session

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	spanneraccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	helpers "github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/helpers"
)

type SessionService struct {
	store   SessionStore
	context context.Context
}

type SessionNameError struct {
	DbName string
	DbType string
}

func (e *SessionNameError) Error() string {
	return fmt.Sprintf("session name already exists for database '%s' and database type '%s'.", e.DbName, e.DbType)

}

func NewSessionService(ctx context.Context, store SessionStore) *SessionService {
	ss := new(SessionService)
	ss.store = store
	ss.context = ctx
	return ss
}

func (ss *SessionService) SaveSession(scs SchemaConversionSession) error {
	unique, err := ss.store.IsSessionNameUnique(ss.context, scs)
	if err != nil {
		return err
	}

	if !unique {
		return &SessionNameError{DbName: scs.DatabaseName, DbType: scs.DatabaseType}
	}

	return ss.store.SaveSession(ss.context, scs)
}

func (ss *SessionService) GetSessionsMetadata() ([]SchemaConversionSession, error) {
	return ss.store.GetSessionsMetadata(ss.context)
}

func (ss *SessionService) GetConvWithMetadata(versionId string) (ConvWithMetadata, error) {
	return ss.store.GetConvWithMetadata(ss.context, versionId)
}

func SetSessionStorageConnectionState(projectId string, spInstanceId string) (bool, bool) {
	sessionState := GetSessionState()
	sessionState.GCPProjectID = projectId
	sessionState.SpannerInstanceID = spInstanceId
	if projectId == "" || spInstanceId == "" {
		sessionState.IsOffline = true
		return false, false
	} else {
		if isDbCreated := helpers.CheckOrCreateMetadataDb(projectId, spInstanceId); isDbCreated {
			sessionState.IsOffline = false
			isConfigValid := isDbCreated
			migrateMetadataDb(projectId, spInstanceId)
			return isDbCreated, isConfigValid
		} else {
			sessionState.IsOffline = true
			return false, false
		}
	}
}

func getOldMetadataDbUri(projectId string, instanceId string) string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, "harbourbridge_metadata")
}

func migrateMetadataDb(projectId, instanceId string) {
	ctx := context.Background()
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer adminClient.Close()

	spA := spanneraccessor.SpannerAccessorImpl{}
	oldMetadataDbUri := getOldMetadataDbUri(projectId, instanceId)
	oldMetadataDBExists, err := spA.CheckExistingDb(ctx, oldMetadataDbUri)
	if err != nil {
		fmt.Printf("could not check if oldMetadataDB exists. error=%v\n", err)
		return
	}
	if !oldMetadataDBExists {
		fmt.Println("Old metadata DB not found.")
		// If old metadata DB doesn't exist, NO_OP
		return
	}

	fmt.Println("Old metadata DB found. Starting migration")

	oldDbSpannerClient, err := spanner.NewClient(ctx, oldMetadataDbUri)
	if err != nil {
		fmt.Printf("could not connect to oldMetadataDB. error=%v\n", err)
		return
	}
	defer oldDbSpannerClient.Close()

	newDbSpannerClient, err := spanner.NewClient(ctx, helpers.GetSpannerUri(projectId, instanceId))
	if err != nil {
		fmt.Printf("could not connect to newMetadataDB. error=%v\n", err)
	}
	defer newDbSpannerClient.Close()

	query := spanner.Statement{
		SQL: `SELECT 
								SessionName,
								EditorName,
								DatabaseType,
								DatabaseName,
								Dialect,
								Notes,
								Tags,
								VersionId,
								PreviousVersionId,
								SchemaChanges,
								TO_JSON_STRING(SchemaConversionObject) AS SchemaConversionObject,
								CreateTimestamp
							FROM SchemaConversionSession `,
	}

	fmt.Println("Querying old Metadata DB.")
	rowIter := oldDbSpannerClient.Single().Query(ctx, query)

	_, err = newDbSpannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		fmt.Println("Writing to new Metadata DB.")
		err := rowIter.Do(func(row *spanner.Row) error {
			var scs SchemaConversionSession
			err := row.ToStruct(&scs)
			if err != nil {
				fmt.Printf("could not read row and parse into struct. error=%v\n", err)
				return err
			}
			mutation, err := spanner.InsertStruct("SchemaConversionSession", scs)
			if err != nil {
				fmt.Printf("count not create Insert mutation. error=%v\n", err)
			}
			return tx.BufferWrite([]*spanner.Mutation{mutation})
		})
		return err
	})
	if err != nil {
		fmt.Printf("could not write to newMetadataDB. error=%v\n", err)
		return
	}

	fmt.Println("Successfully wrote data to new metadata DB.")

	err = adminClient.DropDatabase(ctx, &databasepb.DropDatabaseRequest{
		Database: oldMetadataDbUri,
	})
	if err != nil {
		fmt.Printf("could not drop oldMetadataDB. error=%v\n", err)
		return
	}
	fmt.Println("Successfully dropped old metadata DB")
}
