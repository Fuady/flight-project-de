output "raw_bucket_name" {
  value       = google_storage_bucket.raw.name
  description = "Name of the raw GCS bucket"
}

output "processed_bucket_name" {
  value       = google_storage_bucket.processed.name
  description = "Name of the processed GCS bucket"
}

output "staging_dataset_id" {
  value       = google_bigquery_dataset.staging.dataset_id
  description = "BigQuery staging dataset ID"
}

output "mart_dataset_id" {
  value       = google_bigquery_dataset.mart.dataset_id
  description = "BigQuery mart dataset ID"
}

output "service_account_email" {
  value       = google_service_account.pipeline_sa.email
  description = "Pipeline service account email"
}

output "dataproc_cluster_name" {
  value       = google_dataproc_cluster.spark_cluster.name
  description = "Dataproc cluster name"
}
