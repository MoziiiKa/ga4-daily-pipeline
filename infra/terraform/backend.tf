terraform {
  backend "gcs" {
    bucket  = "mozaffar_kazemi_tfstate"
    prefix  = "ga4/pipeline"
  }
}
