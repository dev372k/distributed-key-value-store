variable "region" {
  default = "us-east-1"
}

variable "instance_count" {
  default = 3
}

variable "instance_type" {
  default = "t3.micro"
}

variable "ami" {
  description = "Ubuntu AMI"
}

variable "key_name" {
  description = "EC2 key pair name"
}

variable "security_group_id" {
  description = "Security group allowing SSH + port 3030"
}