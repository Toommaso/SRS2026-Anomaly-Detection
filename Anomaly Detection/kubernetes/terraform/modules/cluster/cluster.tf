data "oci_core_vcn" "this" {
  vcn_id = var.vcn_id
}

data "oci_core_subnet" "private" {
  subnet_id = var.private_subnet_id
}

data "oci_core_subnet" "public" {
  subnet_id = var.public_subnet_id
}

#control plane
resource "oci_containerengine_cluster" "this" {
  compartment_id     = var.compartment_id
  kubernetes_version = var.kubernetes_version
  name               = "${var.prefix}-cluster"
  vcn_id             = data.oci_core_vcn.this.id

  endpoint_config {
    is_public_ip_enabled = true
    subnet_id            = data.oci_core_subnet.public.id
  }

  options {
    add_ons {
      is_kubernetes_dashboard_enabled = var.enable_dashboard
      is_tiller_enabled               = var.enable_tiller
    }

    service_lb_subnet_ids = [data.oci_core_subnet.public.id]
  }

  freeform_tags = var.freeform_tags
  defined_tags  = var.defined_tags

  lifecycle {
    ignore_changes = [defined_tags, freeform_tags]
  }
}
