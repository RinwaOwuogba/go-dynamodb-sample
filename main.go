package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type MovieItem struct  {
    ReleaseYear int16
    Title string
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
        fmt.Printf("- %s \n", tableName)
    }
}

func CreateTable(svc *dynamodb.Client, tableName string) {
    input := &dynamodb.CreateTableInput{
        AttributeDefinitions: []types.AttributeDefinition{
            {
                AttributeName: aws.String("ReleaseYear"),
                AttributeType: types.ScalarAttributeTypeN,
            },
            {
                AttributeName: aws.String("Title"),
                AttributeType: types.ScalarAttributeTypeS,
            },
        },
        KeySchema: []types.KeySchemaElement{
            {
                AttributeName: aws.String("ReleaseYear"),
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

func PutItem(svc *dynamodb.Client, inputItem MovieItem, tableName string) error {
	input, err := attributevalue.MarshalMap(inputItem)
	if err != nil {
		log.Fatalln("Error marshalling input item")
	}

	finalInput := &dynamodb.PutItemInput{
		Item:                input,
		TableName:           aws.String(tableName),
		ConditionExpression: aws.String("attribute_not_exists(ReleaseYear)"),
	}

	_, err = svc.PutItem(context.TODO(), finalInput)

	if err != nil {
        return err;
	}

	fmt.Printf("Suksexful\n\n")
    return nil;
	// fmt.Println(x.AttributeDefinitions)
}

func main() {
    // set config variables for dev mode - localstack
    os.Setenv("AWS_ACCESS_KEY_ID", "test")
    os.Setenv("AWS_SECRET_ACCESS_KEY", "test")

    opts := dynamodb.Options{
        Region:           "us-east-1",
		EndpointResolver: dynamodb.EndpointResolverFromURL("http://localhost:4566"),
	}
    // Using the config options, create the DynamoDB client
    svc := dynamodb.New(opts);

    // Create table Movies
    tableName := "Movies"

    CreateTable(svc, tableName);
    PrintTables(svc);
    fmt.Println()

    // Put item
    newMovie := MovieItem{
        ReleaseYear: 2022,
        Title: "Dawn of Ice",
    };
    err := PutItem(svc, newMovie, tableName);
    if (err != nil) {
		log.Println("Error putting item in DB: ", err.Error())
    }

    err = PutItem(svc, newMovie, tableName);
    if (err != nil) {   
		log.Println("Error putting item in DB: ", err.Error())
    }
    
    DeleteTable(svc, tableName);
    PrintTables(svc);
}