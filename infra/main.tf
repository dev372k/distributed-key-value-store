provider "aws" {
  region = var.region
}

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