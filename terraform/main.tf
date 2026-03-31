terraform {
  required_version = ">= 1.3"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ─── GCS Buckets ────────────────────────────────────────────────────────────

resource "google_storage_bucket" "raw" {
  name          = "${var.project_id}-flights-raw"
  location      = var.region
  force_destroy = true

  lifecycle_rule {
    condition { age = 90 }
    action { type = "Delete" }
  }

  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "processed" {
  name          = "${var.project_id}-flights-processed"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true
}

# ─── BigQuery Datasets ───────────────────────────────────────────────────────

resource "google_bigquery_dataset" "staging" {
  dataset_id  = "flights_staging"
  location    = var.region
  description = "Staging layer: raw data loaded from GCS"

  labels = {
    env     = var.environment
    project = "flights-de"
  }
}

resource "google_bigquery_dataset" "mart" {
  dataset_id  = "flights_mart"
  location    = var.region
  description = "Mart layer: transformed, partitioned tables for dashboards"

  labels = {
    env     = var.environment
    project = "flights-de"
  }
}

# ─── Service Account ─────────────────────────────────────────────────────────

resource "google_service_account" "pipeline_sa" {
  account_id   = "flights-pipeline-sa"
  display_name = "Flights DE Pipeline Service Account"
}

resource "google_project_iam_member" "sa_gcs_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "sa_bq_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "sa_dataproc" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_service_account_key" "pipeline_sa_key" {
  service_account_id = google_service_account.pipeline_sa.name
}

resource "local_file" "sa_key_file" {
  content  = base64decode(google_service_account_key.pipeline_sa_key.private_key)
  filename = "${path.module}/../keys/sa-key.json"
}

# ─── Dataproc Cluster (Spark) ────────────────────────────────────────────────

resource "google_dataproc_cluster" "spark_cluster" {
  name   = "flights-spark-cluster"
  region = var.region

  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "n1-standard-4"
      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = 50
      }
    }

    worker_config {
      num_instances = 2
      machine_type  = "n1-standard-4"
      disk_config {
        boot_disk_type    = "pd-standard"
        boot_disk_size_gb = 100
      }
    }

    software_config {
      image_version = "2.1-debian11"
      optional_components = ["JUPYTER"]
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
    }

    gce_cluster_config {
      service_account = google_service_account.pipeline_sa.email
      service_account_scopes = ["cloud-platform"]
    }
  }
}
