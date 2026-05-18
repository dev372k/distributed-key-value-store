output "public_ips" {
  value = aws_instance.kv_nodes[*].public_ip
}

output "private_ips" {
  value = aws_instance.kv_nodes[*].private_ip
}

output "sns_topic_arn" {
  value = aws_sns_topic.kv_topic.arn
}

output "sqs_queue_urls" {
  value = [for q in aws_sqs_queue.kv_queues : q.id]
}