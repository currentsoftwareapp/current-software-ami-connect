# Custom parameter group that enables the pgaudit extension so database
# activity is audited. pgaudit must be loaded at server start via
# shared_preload_libraries (a static parameter that requires a reboot), and
# pgaudit.log must capture at least one class of SQL statements.
resource "aws_db_parameter_group" "ami_connect_airflow_metastore" {
  name        = "ami-connect-airflow-db-pg16"
  family      = "postgres16"
  description = "Postgres 16 parameter group for the AMI Connect Airflow metastore with pgaudit auditing enabled"

  parameter {
    name         = "shared_preload_libraries"
    value        = "pgaudit"
    apply_method = "pending-reboot"
  }

  # Audit DDL, role, and write (INSERT/UPDATE/DELETE/TRUNCATE) statements.
  parameter {
    name         = "pgaudit.log"
    value        = "ddl,role,write"
    apply_method = "immediate"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_db_instance" "ami_connect_airflow_metastore" {
  identifier                      = "ami-connect-airflow-db"
  engine                          = "postgres"
  engine_version                  = "16.13"
  instance_class                  = "db.t4g.micro"
  allocated_storage               = 100
  storage_type                    = "gp3"
  storage_encrypted               = true
  multi_az                        = true
  backup_retention_period         = 7
  deletion_protection             = true
  enabled_cloudwatch_logs_exports = ["postgresql"]
  db_name                         = "airflow_db"
  username                        = "airflow_user"
  password                        = var.airflow_db_password
  vpc_security_group_ids          = [aws_security_group.airflow_db_sg.id]
  parameter_group_name            = aws_db_parameter_group.ami_connect_airflow_metastore.name
  skip_final_snapshot             = false
  final_snapshot_identifier       = "final-snapshot-ami-connect-airflow-db"
}