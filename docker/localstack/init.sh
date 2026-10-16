#!/bin/sh
set -eu
DLQ_URL=$(awslocal sqs create-queue --queue-name wager-transactions-dlq.fifo --attributes FifoQueue=true --query QueueUrl --output text)
DLQ_ARN=$(awslocal sqs get-queue-attributes --queue-url "$DLQ_URL" --attribute-names QueueArn --query Attributes.QueueArn --output text)
awslocal sqs create-queue --queue-name wager-transactions.fifo --attributes "{\"FifoQueue\":\"true\",\"VisibilityTimeout\":\"30\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$DLQ_ARN\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}\"}"
awslocal sqs create-queue --queue-name wager-events.fifo --attributes FifoQueue=true
