resource "google_storage_bucket" "landing" {
  name     = "platform_assignment_bucket"
  location = "EUROPE-WEST4"

  uniform_bucket_level_access = true         # UBLA

  force_destroy               = false        # protect against rm ‑rf accidents

  versioning {
    enabled = true                           # keeps object history
  }

  retention_policy {                         # soft‑delete window
    retention_period = 604800                # 7 days in seconds
    is_locked        = false                 
  }

}
