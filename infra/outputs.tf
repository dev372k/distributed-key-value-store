output "public_ips" {
  value = aws_instance.kv_nodes[*].public_ip
}

output "private_ips" {
  value = aws_instance.kv_nodes[*].private_ip
}