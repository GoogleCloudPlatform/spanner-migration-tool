package dynamodb

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// NewDynamoDBStream initializes a new DynamoDB Stream for a table with NEW_AND_OLD_IMAGES
// StreamViewType. If there exists a stream for a given table then it must be of type
// NEW_IMAGE or NEW_AND_OLD_IMAGES otherwise this table is dropped from streaming migration.
// It returns latest Stream Arn for the table along with any error if encountered.
func NewDynamoDBStream(client dynamodbiface.DynamoDBAPI, srcTable string) (string, error) {
	describeTableInput := &dynamodb.DescribeTableInput{
		TableName: aws.String(srcTable),
	}
	result, err := client.DescribeTable(describeTableInput)
	if err != nil {
		return "", err
	}
	if result.Table.StreamSpecification != nil {
		switch *result.Table.StreamSpecification.StreamViewType {
		case dynamodb.StreamViewTypeKeysOnly:
			return "", fmt.Errorf("error! there exists a stream with KEYS_ONLY StreamViewType")
		case dynamodb.StreamViewTypeOldImage:
			return "", fmt.Errorf("error! there exists a stream with OLD_IMAGE StreamViewType")
		default:
			return *result.Table.LatestStreamArn, nil
		}
	} else {
		streamSpecification := &dynamodb.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: aws.String(dynamodb.StreamViewTypeNewAndOldImages),
		}
		updateTableInput := &dynamodb.UpdateTableInput{
			StreamSpecification: streamSpecification,
			TableName:           aws.String(srcTable),
		}
		res, err := client.UpdateTable(updateTableInput)
		if err != nil {
			return "", err
		}
		return *res.TableDescription.LatestStreamArn, nil
	}
}
