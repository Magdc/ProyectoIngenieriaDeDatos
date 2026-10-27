# Glue Data Catalog del lake (Lote 4.1, HU-007; ARCHITECTURE.md §3.19 y §5.7).
#
# Bases ta_raw, ta_trusted y ta_curated y sus tablas EXTERNAL Parquet. Las tablas salen de
# tables.json, que se genera desde project/contracts/athena/*.sql (python -m lakehouse.catalog_spec
# --write; una prueba exige que estén al día, §5.13). Las particiones se resuelven con partition
# projection (parámetros projection.* de cada tabla): sin crawlers ni particiones registradas.
# Lote 5.3 (HU-034, §3.25): la descripción de cada tabla y el comentario de cada columna y clave de partición
# también salen de las DDL (con su clase de privacidad); aquí solo se recortan a los límites de Glue.

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "bucket_name" {
  description = "Bucket del lake: reemplaza {{lake_bucket}} de las DDL."
  type        = string
}

variable "databases" {
  description = "Capa → base de Glue. Cada tabla de tables.json debe caer en una de estas bases."
  type        = map(string)
  default = {
    raw     = "ta_raw"
    trusted = "ta_trusted"
    curated = "ta_curated"
  }
}

variable "tables_file" {
  description = "JSON generado desde contracts/athena/ (por defecto, el del módulo)."
  type        = string
  default     = ""
}

locals {
  spec     = jsondecode(file(var.tables_file != "" ? var.tables_file : "${path.module}/tables.json"))
  tables   = { for t in local.spec.tables : "${t.database}.${t.name}" => t }
  db_layer = { for layer, name in var.databases : name => layer }
}

resource "aws_glue_catalog_database" "this" {
  for_each = var.databases

  name         = each.value
  description  = "TrendAnalyzer: capa ${each.key} del lake (ARCHITECTURE.md §5.7)"
  location_uri = "s3://${var.bucket_name}/${each.key}/"
}

resource "aws_glue_catalog_table" "this" {
  for_each = local.tables

  database_name = aws_glue_catalog_database.this[local.db_layer[each.value.database]].name
  name          = each.value.name
  description   = substr(replace(each.value.description, "{{lake_bucket}}", var.bucket_name), 0, 2048)
  table_type    = "EXTERNAL_TABLE"

  parameters = merge(
    { for k, v in each.value.parameters : k => replace(v, "{{lake_bucket}}", var.bucket_name) },
    { EXTERNAL = "TRUE", classification = "parquet" },
  )

  dynamic "partition_keys" {
    for_each = each.value.partition_keys
    content {
      name    = partition_keys.value.name
      type    = partition_keys.value.type
      comment = partition_keys.value.comment != "" ? substr(partition_keys.value.comment, 0, 255) : null
    }
  }

  storage_descriptor {
    location      = replace(each.value.location, "{{lake_bucket}}", var.bucket_name)
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    dynamic "columns" {
      for_each = each.value.columns
      content {
        name    = columns.value.name
        type    = columns.value.type
        comment = columns.value.comment != "" ? substr(columns.value.comment, 0, 255) : null
      }
    }
  }
}

output "database_names" {
  description = "Capa → nombre de la base (raw, trusted, curated)."
  value       = { for layer, db in aws_glue_catalog_database.this : layer => db.name }
}

output "table_names" {
  description = "Tablas creadas (base.tabla)."
  value       = sort([for k, t in aws_glue_catalog_table.this : "${t.database_name}.${t.name}"])
}

output "table_locations" {
  description = "base.tabla → ubicación en S3."
  value       = { for k, t in aws_glue_catalog_table.this : k => t.storage_descriptor[0].location }
}
