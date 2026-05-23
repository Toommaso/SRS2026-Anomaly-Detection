variable "compartment_id" {
  type        = string
  description = "compartment id where to create all resources."
}

variable "enable_ipv6" {
  type        = bool
  description = "Whether IPv6 is enabled for the VCN. If enabled, Oracle will assign the VCN a IPv6 /56 CIDR block."
}

variable "prefix" {
  type        = string
  description = "The prefix of all resources name."
}

variable "vcn_cidr" {
  type        = string
  description = "The IPv4 CIDR blocks of the VCN."
}

variable "private_subnet_cidr" {
  type        = string
  description = "The IPv4 CIDR block of the private subnet."
}

variable "public_subnet_cidr" {
  type        = string
  description = "The IPv4 CIDR block of the public subnet."
}

variable "public_subnet_open_ports" {
  type        = set(number)
  description = "The list of ports opened in public subnet."
}

variable "freeform_tags" {
  type        = map(any)
  description = "simple key-value pairs to tag the created resources using freeform OCI Free-form tags."
}

variable "defined_tags" {
  type        = map(string)
  description = "predefined and scoped to a namespace to tag the resources created using defined tags."
}