package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type EndPointResolverFunc func(service, region string, options ...interface{}) (aws.Endpoint, error)

func (e EndPointResolverFunc) ResolveEndpoint(service, region string, options ...interface{})  (aws.Endpoint, error) {
    return e(service, region, options);
};

func DummyResolver(service, region string, options ...interface{})  (aws.Endpoint, error) {
    endpoint := aws.Endpoint{
        URL: "http://localhost:4566",
        Source: aws.EndpointSourceCustom,
        SigningRegion: region,
    };
    return endpoint, nil;
}

func PrintTables(svc *dynamodb.Client) {
    // Build the request with its input parameters
    resp, err := svc.ListTables(context.TODO(), &dynamodb.ListTablesInput{
        Limit: aws.Int32(5),
    })
    if err != nil {
        log.Fatalf("failed to list tables, %v", err)
    }

    fmt.Println("Tables:")
    for _, tableName := range resp.TableNames {
        fmt.Println(tableName)
    }
}

func CreateTable(svc *dynamodb.Client, tableName string) {
    

    input := &dynamodb.CreateTableInput{
        AttributeDefinitions: []types.AttributeDefinition{
            {
                AttributeName: aws.String("Year"),
                AttributeType: types.ScalarAttributeTypeN,
            },
            {
                AttributeName: aws.String("Title"),
                AttributeType: types.ScalarAttributeTypeS,
            },
        },
        KeySchema: []types.KeySchemaElement{
            {
                AttributeName: aws.String("Year"),
                KeyType:       types.KeyTypeHash,
            },
            {
                AttributeName: aws.String("Title"),
                KeyType:       types.KeyTypeRange,
            },
        },
        ProvisionedThroughput: &types.ProvisionedThroughput{
            ReadCapacityUnits:  aws.Int64(10),
            WriteCapacityUnits: aws.Int64(10),
        },
        TableName: aws.String(tableName),
    }
    
    _, err := svc.CreateTable(context.TODO(), input)
    if err != nil {
        log.Fatalf("Got error calling CreateTable: %s", err)
    }
    fmt.Println("Created the table", tableName)
}

func DeleteTable(svc *dynamodb.Client, tableName string) {
    input := &dynamodb.DeleteTableInput{
   
        TableName: aws.String(tableName),
    }
    
    _, err := svc.DeleteTable(context.TODO(), input)

    if err != nil {
        log.Fatalf("Got error calling DeleteTable: %s", err)
    }
    fmt.Println("Deleted the table", tableName)
}

func main() {
    // set config variables for dev mode - localstack
    os.Setenv("AWS_ACCESS_KEY_ID", "test")
    os.Setenv("AWS_SECRET_ACCESS_KEY", "test")

    // Using the SDK's default configuration, loading additional config
    // and credentials values from the environment variables, shared
    // credentials, and shared configuration files
    cfg, err := config.LoadDefaultConfig(context.TODO(),
   		config. WithRegion("us-west-2"),
        config.WithEndpointResolverWithOptions(EndPointResolverFunc(DummyResolver)),
   	)

    if err != nil {
        log.Fatalf("unable to load SDK config, %v", err)
    }
	

    // Using the Config value, create the DynamoDB client
    svc := dynamodb.NewFromConfig(cfg)

    // Create table Movies
    tableName := "Movies"
    CreateTable(svc, tableName);
    PrintTables(svc);

    fmt.Println()
    
    DeleteTable(svc, tableName);
    PrintTables(svc);
}