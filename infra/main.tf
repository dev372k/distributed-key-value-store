provider "aws" {
  region = var.region
}

# ---------------- EC2 INSTANCES ----------------
resource "aws_instance" "kv_nodes" {
  count         = var.instance_count
  ami           = var.ami
  instance_type = var.instance_type
  key_name      = var.key_name

  vpc_security_group_ids = [var.security_group_id]

  tags = {
    Name = "kv-node-${count.index}"
  }
}

# ---------------- SNS TOPIC ----------------
resource "aws_sns_topic" "kv_topic" {
  name = "kv-replication-topic"
}

# ---------------- SQS QUEUES (ONE PER NODE) ----------------
resource "aws_sqs_queue" "kv_queues" {
  count = var.instance_count

  name = "kv-queue-${count.index}"

  visibility_timeout_seconds = 30
  message_retention_seconds  = 86400
}

# ---------------- SQS POLICY (ALLOW SNS → SQS) ----------------
resource "aws_sqs_queue_policy" "kv_queue_policy" {
  count = var.instance_count

  queue_url = aws_sqs_queue.kv_queues[count.index].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = "*"
        Action = "sqs:SendMessage"
        Resource = aws_sqs_queue.kv_queues[count.index].arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = aws_sns_topic.kv_topic.arn
          }
        }
      }
    ]
  })
}

# ---------------- SNS → SQS SUBSCRIPTIONS ----------------
resource "aws_sns_topic_subscription" "kv_subscriptions" {
  count = var.instance_count

  topic_arn = aws_sns_topic.kv_topic.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.kv_queues[count.index].arn
}

