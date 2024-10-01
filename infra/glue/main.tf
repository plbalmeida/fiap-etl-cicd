# role que será usada nos jobs
data "aws_iam_role" "glue_role" {
  name = var.role_name
}

resource "aws_glue_job" "extract_job" {
  name     = "job-extract"
  role_arn = data.aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "${var.script_location}extract.py"
  }

  default_arguments = var.default_arguments

  max_retries       = 0
  worker_type       = "G.1X"
  number_of_workers = 2

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_glue_job" "transform_job" {
  name     = "job-transform"
  role_arn = data.aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "${var.script_location}transform.py"
  }

  default_arguments = merge(
    var.default_arguments,
    { "--extra-py-files" = "s3://fiap-etl-20240918/libs/utils.zip" }
  )

  max_retries       = 0
  worker_type       = "G.1X"
  number_of_workers = 2

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_glue_job" "load_job" {
  name     = "job-load"
  role_arn = data.aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "${var.script_location}load.py"
  }

  default_arguments = var.default_arguments

  max_retries       = 0
  worker_type       = "G.1X"
  number_of_workers = 2

  lifecycle {
    create_before_destroy = true
  }
}
