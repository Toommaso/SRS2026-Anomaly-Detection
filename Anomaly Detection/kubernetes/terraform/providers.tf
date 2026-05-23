terraform {
  required_providers {
    oci = {
      source  = "oracle/oci"
      version = "~> 7.2.0"
    }

    # kubernetes = {
    #   source  = "hashicorp/kubernetes"
    #   version = "~> 2.37"
    # }

  #   helm = {
  #     source  = "hashicorp/helm"
  #     version = "~> 2.17"
  #   }

  #   tls = {
  #     source  = "hashicorp/tls"
  #     version = "~> 4.1.0"
  #   }

  #   cloudflare = {
  #     source  = "cloudflare/cloudflare"
  #     version = "~> 5.5.0"
  #   }
  }
}

provider "oci" {
  config_file_profile = "DEFAULT"
}

# provider "cloudflare" {
#   api_token = var.cf_api_token
# }