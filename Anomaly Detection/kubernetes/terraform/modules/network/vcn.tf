resource "oci_core_vcn" "this" {
  compartment_id = var.compartment_id
  display_name   = "${var.prefix}-vcn"
  dns_label      = "${var.prefix}vcn"
  is_ipv6enabled = var.enable_ipv6
  cidr_block     = var.vcn_cidr

  freeform_tags = var.freeform_tags
  defined_tags  = var.defined_tags

  lifecycle {
    ignore_changes = [defined_tags, dns_label, freeform_tags]
  }
}