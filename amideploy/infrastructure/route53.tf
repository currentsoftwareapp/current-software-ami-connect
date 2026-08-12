data "aws_route53_zone" "airflow_route53_zone" {
  name = var.airflow_hostname
}

# Query logging for the public hosted zone. Route 53 only supports logging to a
# CloudWatch Logs log group in us-east-1, so the log group and its resource
# policy are created with the us_east_1 provider alias.
resource "aws_cloudwatch_log_group" "route53_query_log" {
  provider          = aws.us_east_1
  name              = "/aws/route53/${var.airflow_hostname}"
  retention_in_days = 90
}

# Allow the Route 53 service to write query logs into the log group.
data "aws_iam_policy_document" "route53_query_log" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["route53.amazonaws.com"]
    }

    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]

    # Route 53 writes to log streams under this group.
    resources = ["${aws_cloudwatch_log_group.route53_query_log.arn}:*"]
  }
}

resource "aws_cloudwatch_log_resource_policy" "route53_query_log" {
  provider        = aws.us_east_1
  policy_name     = "ami-connect-route53-query-logging"
  policy_document = data.aws_iam_policy_document.route53_query_log.json
}

resource "aws_route53_query_log" "airflow_route53_zone" {
  zone_id                  = data.aws_route53_zone.airflow_route53_zone.zone_id
  cloudwatch_log_group_arn = aws_cloudwatch_log_group.route53_query_log.arn

  depends_on = [aws_cloudwatch_log_resource_policy.route53_query_log]
}

resource "aws_route53_record" "www" {
  zone_id = data.aws_route53_zone.airflow_route53_zone.zone_id
  name    = "www"
  type    = "A"
  ttl     = 300
  records = [aws_eip.ami_connect_airflow_server_ip.public_ip]
}

resource "aws_route53_record" "root" {
  zone_id = data.aws_route53_zone.airflow_route53_zone.zone_id
  name    = ""
  type    = "A"
  ttl     = 300
  records = [aws_eip.ami_connect_airflow_server_ip.public_ip]
}