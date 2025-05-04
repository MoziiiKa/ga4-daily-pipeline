terraform {
  backend "gcs" {
    bucket  = "Mozaffar-Kazemi-tfstate"
    prefix  = "ga4/pipeline"
  }
}
