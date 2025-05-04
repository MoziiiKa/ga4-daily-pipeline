terraform {
  backend "gcs" {
    bucket  = "Mozaffar_Kazemi_tfstate"
    prefix  = "ga4/pipeline"
  }
}
