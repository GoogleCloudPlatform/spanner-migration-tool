package oracle

import (
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"database/sql"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	_ "github.com/sijms/go-ora/v2" // Oracle driver
)

type InfoSchemaImpl struct {
	DbName string
	Db     *sql.DB
}

func (isi InfoSchemaImpl) GetTriggerInfo() ([]utils.TriggerAssessmentInfo, error) {
	q := "SELECT TRIGGER_NAME, TABLE_NAME, TRIGGERING_EVENT FROM ALL_TRIGGERS WHERE OWNER = UPPER(:1)"
	rows, err := isi.Db.Query(q, isi.DbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var triggerInfo []utils.TriggerAssessmentInfo
	var triggerName, tableName, event string

	for rows.Next() {
		err := rows.Scan(&triggerName, &tableName, &event)
		if err == nil {
			triggerInfo = append(triggerInfo, utils.TriggerAssessmentInfo{
				Name:              triggerName,
				TargetTable:       tableName,
				EventManipulation: event,
			})
		}
	}
	return triggerInfo, nil
}

func (isi InfoSchemaImpl) GetFunctionInfo() ([]utils.FunctionAssessmentInfo, error) {
	return []utils.FunctionAssessmentInfo{}, nil
}

func (isi InfoSchemaImpl) GetStoredProcedureInfo() ([]utils.StoredProcedureAssessmentInfo, error) {
	return []utils.StoredProcedureAssessmentInfo{}, nil
}

func (isi InfoSchemaImpl) GetViewInfo() ([]utils.ViewAssessmentInfo, error) {
	return []utils.ViewAssessmentInfo{}, nil
}

func (isi InfoSchemaImpl) GetIndexInfo(table string, index schema.Index) (utils.IndexAssessmentInfo, error) {
	return utils.IndexAssessmentInfo{}, nil
}

func (isi InfoSchemaImpl) GetTableInfo(conv *internal.Conv) (map[string]utils.TableAssessmentInfo, error) {
	return make(map[string]utils.TableAssessmentInfo), nil
}
