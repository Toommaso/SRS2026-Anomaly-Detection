# variable "region" {
#   # List of regions: https://docs.cloud.oracle.com/iaas/Content/General/Concepts/regions.htm#ServiceAvailabilityAcrossRegions
#   description = "the OCI region where resources will be created"
#   type        = string
# }

variable "compartment_id" {
  type        = string
  description = "The compartment to create all resources in"
}

# variable "ssh_public_key" {
#   type        = string
#   description = "The public key for SSH to worker node."
# }

variable "user_id" {
  type        = string
  description = "OCID dell'utente OCI usato per generare l'auth token OCIR"
}

variable "k8s_namespace" {
  type        = string
  description = "k8s namespace name"
}

variable "freeform_tags" {
  description = "simple key-value pairs to tag the created resources using freeform OCI Free-form tags."
  type        = map(any)
  default = {
    deployed_by = "terraform"
  }
}

variable "defined_tags" {
  description = "predefined and scoped to a namespace to tag the resources created using defined tags."
  type        = map(string)
  default     = null
}

# variable "cf_zone_id" {
#   type        = string
#   description = "the id of the zone to create the origin ca certificate in"
# }

# variable "cf_api_token" {
#   type        = string
#   description = "the api token to create the origin ca certificate in"
# }
