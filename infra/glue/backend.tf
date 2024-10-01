terraform {
  backend "s3" {
    region         = "us-east-1"
    role_arn       = "arn:aws:iam::413467296690:role/fiap-etl-terraform-backend-role"
    bucket         = "fiap-etl-terraform-backend-bucket"
    key            = "terraform.tfstate"
    dynamodb_table = "fiap-etl-terraform-backend-lock-table"
    encrypt        = true
  }
}
