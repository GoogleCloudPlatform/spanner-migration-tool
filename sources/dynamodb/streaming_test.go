package dynamodb

import (
	"context"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/stretchr/testify/assert"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// tests StartChangeDataCapture and NewDynamoDBStream functions
func TestInfoSchemaImpl_StartChangeDataCapture(t *testing.T) {
	tableName := "testtable"
	attrNameA := "a"
	latestStreamArn := "arn:aws:dynamodb:dydb_endpoint:test_stream"

	cols := []string{attrNameA}
	spSchema := ddl.CreateTable{
		Name:     tableName,
		ColNames: cols,
		ColDefs: map[string]ddl.ColumnDef{
			attrNameA: {Name: attrNameA, T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		},
		Pks: []ddl.IndexKey{{Col: attrNameA}},
	}
	srcTable := schema.Table{
		Name:     tableName,
		ColNames: cols,
		ColDefs: map[string]schema.Column{
			attrNameA: {Name: attrNameA, Type: schema.Type{Name: typeString}},
		},
		PrimaryKeys: []schema.Key{{Column: attrNameA}},
	}
	conv := buildConv(spSchema, srcTable)
	type fields struct {
		DynamoClient        dynamodbiface.DynamoDBAPI
		DynamoStreamsClient *dynamodbstreams.DynamoDBStreams
		SampleSize          int64
	}
	type args struct {
		ctx  context.Context
		conv *internal.Conv
	}
	arguments := args{
		ctx:  context.Background(),
		conv: conv,
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string]interface{}
		wantErr bool
	}{
		{
			name: "test for checking correctness of output when stream exists already",
			fields: fields{
				DynamoClient: &mockDynamoClient{
					describeTableOutputs: []dynamodb.DescribeTableOutput{
						{
							Table: &dynamodb.TableDescription{
								TableName: &tableName,
								StreamSpecification: &dynamodb.StreamSpecification{
									StreamEnabled:  aws.Bool(true),
									StreamViewType: aws.String(dynamodb.StreamViewTypeNewImage),
								},
								LatestStreamArn: &latestStreamArn,
							},
						},
					},
				},
			},
			args: arguments,
			want: map[string]interface{}{
				tableName: latestStreamArn,
			},
			wantErr: false,
		},
		{
			name: "test for checking correctness of output when a new stream is created",
			fields: fields{
				DynamoClient: &mockDynamoClient{
					describeTableOutputs: []dynamodb.DescribeTableOutput{
						{
							Table: &dynamodb.TableDescription{
								TableName: &tableName,
							},
						},
					},
					updateTableOutputs: []dynamodb.UpdateTableOutput{
						{
							TableDescription: &dynamodb.TableDescription{
								LatestStreamArn: &latestStreamArn,
								StreamSpecification: &dynamodb.StreamSpecification{
									StreamEnabled:  aws.Bool(true),
									StreamViewType: aws.String(dynamodb.StreamViewTypeNewAndOldImages),
								},
							},
						},
					},
				},
			},
			args: arguments,
			want: map[string]interface{}{
				tableName: latestStreamArn,
			},
			wantErr: false,
		},
		{
			name: "test for handling api calls failure",
			fields: fields{
				DynamoClient: &mockDynamoClient{
					describeTableOutputs: []dynamodb.DescribeTableOutput{
						{
							Table: &dynamodb.TableDescription{
								TableName: &tableName,
							},
						},
					},
				},
			},
			args:    arguments,
			want:    map[string]interface{}{},
			wantErr: false,
		},
	}
	totalUnexpecteds := int64(0)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isi := InfoSchemaImpl{
				DynamoClient:        tt.fields.DynamoClient,
				DynamoStreamsClient: tt.fields.DynamoStreamsClient,
				SampleSize:          tt.fields.SampleSize,
			}
			got, err := isi.StartChangeDataCapture(tt.args.ctx, tt.args.conv)
			if (err != nil) != tt.wantErr {
				t.Errorf("InfoSchemaImpl.StartChangeDataCapture() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("InfoSchemaImpl.StartChangeDataCapture() = %v, want %v", got, tt.want)
			}
			totalUnexpecteds += conv.Unexpecteds()
		})
	}
	assert.Equal(t, int64(1), totalUnexpecteds)
}
