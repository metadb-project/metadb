package snsnotifier

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/metadb-project/metadb/cmd/metadb/log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

const matchSNSRegion = `^(?:[^:]+:){3}([^:]+).*`
const snsTopicPrefix = "arn:aws:sns:"

type snsNotifier struct {
	client *sns.Client
	topic  string
}

func NewSNS(topic string) (*snsNotifier, error) {
	if len(topic) == 0 {
		return nil, errors.New("no topic provided for SNS")
	}

	// Get AWS Region from topic ARN
	re := regexp.MustCompile(matchSNSRegion)
	match := re.FindStringSubmatch(topic)
	if len(match) < 2 {
		return nil, fmt.Errorf("unable to get region from topic arn: %s", topic)
	}

	// Load credentials: https://aws.github.io/aws-sdk-go-v2/docs/configuring-sdk/
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(match[1]))
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config: %w", err)
	}

	snsClient := sns.NewFromConfig(cfg)
	return &snsNotifier{snsClient, topic}, nil
}

func (n *snsNotifier) Notify(ctx context.Context, message string) error {
	if n == nil || n.client == nil {
		return nil
	}

	res, err := n.client.Publish(ctx, &sns.PublishInput{
		Message:  aws.String(message),
		TopicArn: aws.String(n.topic),
	})
	if err != nil {
		return nil
	}

	log.Debug("message sent to SNS: %s", *res.MessageId)

	return nil
}

// Check if notification string is SNS topic
func IsSNSTopic(s string) bool {
	return strings.HasPrefix(s, snsTopicPrefix)
}
