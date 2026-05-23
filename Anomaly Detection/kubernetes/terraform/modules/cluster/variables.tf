variable "compartment_id" {
  type        = string
  description = "compartment id where to create all resources."
}

variable "kubernetes_version" {
  type        = string
  description = "version of the kubernetes cluster."
}

variable "prefix" {
  type        = string
  description = "The prefix of all resources name."
}

variable "vcn_id" {
  type        = string
  description = "VCN id the kubernetes cluster host in."
}

variable "private_subnet_id" {
  type        = string
  description = "The id of the private subnet."
}

variable "public_subnet_id" {
  type        = string
  description = "The id of the public subnet."
}

variable "enable_dashboard" {
  type        = bool
  description = "Enable Kubernetes Dashboard."
  default     = true
}

variable "enable_tiller" {
  type        = bool
  description = "Enable Tiller."
  default     = true
}

variable "node_pool_size" {
  type        = number
  description = "The size of the node pool."
  default     = 2
}

variable "node_shape" {
  type        = string
  description = "The shape of the node pool."
  default     = "VM.Standard.A1.Flex"
}

variable "node_shape_config" { #max 4cpu and 24gb ram free tier
  type        = map(any)
  description = "The shape of the node pool."
  default = {
    memory_in_gbs = 6
    ocpus         = 1
  }
}

variable "shape_boot_volume_size" {
  type        = number
  description = "The size of each shape boot's volume in gbs."
  default     = 50
}

variable "ssh_public_key" {
  type        = string
  description = "The public key for SSH to the worker nodes."
  default     = null
}

variable "freeform_tags" {
  type        = map(any)
  description = "simple key-value pairs to tag the created resources using freeform OCI Free-form tags."
}

variable "defined_tags" {
  type        = map(string)
  description = "predefined and scoped to a namespace to tag the resources created using defined tags."
}