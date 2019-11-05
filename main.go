package main

import (
	"fmt"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func main() {
	if len(os.Args) != 2 {
		exitErrorf("Delete all s3 tags from all versions. \n Error: Bucket name required\nUsage: %s bucket_name",
			os.Args[0])
	}

	bucket := os.Args[1]

	// Initialize a session in us-west-2 that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials.
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("eu-west-1")},
	)

	// Create S3 service client
	svc := s3.New(sess)
	var count uint64
	params := s3.ListObjectVersionsInput{Bucket: aws.String(bucket)}
	err = svc.ListObjectVersionsPages(&params,
		func(page *s3.ListObjectVersionsOutput, lastPage bool) bool {
			var wg sync.WaitGroup
			for _, v := range page.Versions {
				i := s3.DeleteObjectTaggingInput{Bucket: aws.String(bucket), Key: v.Key, VersionId: v.VersionId}
				wg.Add(1)
				go deleteTags(svc, &i, &wg)
				count = count + 1
				if count%10000 == 0 {
					fmt.Fprintf(os.Stderr, "deleted %d objects\n", count)
				}
			}
			wg.Wait()
			return true
		})

	if err != nil {
		exitErrorf("fail", err)
	}

}

func deleteTags(svc *s3.S3, i *s3.DeleteObjectTaggingInput, wg *sync.WaitGroup) {
	defer wg.Done()
	_, err := svc.DeleteObjectTagging(i)

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
	}
}
func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
