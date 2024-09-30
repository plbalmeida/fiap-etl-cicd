variable "role_name" {
  default = "fiap-etl-role"
}

variable "script_location" {
  default = "s3://fiap-etl-20240918/scripts/"
}

variable "default_arguments" {
  type = map(string)
  default = {
    "--job-bookmark-option"       = "job-bookmark-disable"
    "--enable-glue-datacatalog"   = "true"
    "--additional-python-modules" = "ipeadatapy"
  }
}
