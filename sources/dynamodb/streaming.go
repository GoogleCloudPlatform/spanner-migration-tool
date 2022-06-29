package dynamodb

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// NewDynamoDBStream initializes a new DynamoDB Stream for a table with NEW_IMAGE
// StreamViewType. If there was already an enabled stream for a given table
// then it first disables it and creates a new one. It returns latest Stream
// Arn for the table along with any error if encountered.
func NewDynamoDBStream(client dynamodbiface.DynamoDBAPI, srcTable string) (string, error) {
	describeTableInput := &dynamodb.DescribeTableInput{
		TableName: aws.String(srcTable),
	}
	result, err := client.DescribeTable(describeTableInput)
	if err != nil {
		return "", err
	}
	if result.Table.StreamSpecification != nil {
		streamSpecification := &dynamodb.StreamSpecification{
			StreamEnabled: aws.Bool(false),
		}
		updateTableInput := &dynamodb.UpdateTableInput{
			StreamSpecification: streamSpecification,
			TableName:           aws.String(srcTable),
		}
		_, err = client.UpdateTable(updateTableInput)
		if err != nil {
			return "", err
		}
	}
	streamSpecification := &dynamodb.StreamSpecification{
		StreamEnabled:  aws.Bool(true),
		StreamViewType: aws.String(dynamodb.StreamViewTypeNewImage),
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
