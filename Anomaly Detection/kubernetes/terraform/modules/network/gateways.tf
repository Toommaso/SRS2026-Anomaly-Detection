# Internet Gateway (IGW) - routing to/from internet
resource "oci_core_internet_gateway" "igw" {
  compartment_id = var.compartment_id
  vcn_id         = oci_core_vcn.this.id
  display_name   = "${var.prefix}-internet-gateway"

  freeform_tags = var.freeform_tags
  defined_tags  = var.defined_tags

  lifecycle {
    ignore_changes = [defined_tags, freeform_tags]
  }
}

resource "oci_core_route_table" "igw" {
  compartment_id = var.compartment_id
  vcn_id         = oci_core_vcn.this.id
  display_name   = "${var.prefix}-igw-route-table"

  route_rules { #ipv4
    destination       = local.anywhere
    network_entity_id = oci_core_internet_gateway.igw.id
    description       = "Internet Gateway as default gateway"
  }

  dynamic "route_rules" { #ipv6
    for_each = var.enable_ipv6 == true ? [1] : []

    content {
      destination       = local.anywhere_ipv6
      network_entity_id = oci_core_internet_gateway.igw.id
      description       = "Internet Gateway as default gateway"
    }
  }

  freeform_tags = var.freeform_tags
  defined_tags  = var.defined_tags

  lifecycle {
    ignore_changes = [defined_tags, freeform_tags]
  }
}

# NAT Gateway (NGW) - NAT for private network nodes
resource "oci_core_public_ip" "ngw_public_ip" {
  compartment_id = var.compartment_id
  display_name   = "${var.prefix}-ngw-reserved-ip"
  lifetime       = "RESERVED"

  freeform_tags = var.freeform_tags
  defined_tags  = var.defined_tags

  lifecycle {
    ignore_changes = [defined_tags, freeform_tags]
  }
}

resource "oci_core_nat_gateway" "ngw" {
  compartment_id = var.compartment_id
  vcn_id         = oci_core_vcn.this.id
  display_name   = "${var.prefix}-nat-gateway"
  public_ip_id   = oci_core_public_ip.ngw_public_ip.id

  freeform_tags = var.freeform_tags
  defined_tags  = var.defined_tags

  lifecycle {
    ignore_changes = [defined_tags, freeform_tags]
  }
}

resource "oci_core_route_table" "ngw" {
  compartment_id = var.compartment_id
  vcn_id         = oci_core_vcn.this.id
  display_name   = "${var.prefix}-ngw-route-table"

  route_rules {
    destination       = local.anywhere
    destination_type  = "CIDR_BLOCK"
    network_entity_id = oci_core_nat_gateway.ngw.id
    description       = "NAT Gateway as default gateway"
  }

  # route_rules {
  #   destination       = lookup(data.oci_core_services.all_oci_services.services[0], "cidr_block")
  #   destination_type  = "SERVICE_CIDR_BLOCK"
  #   network_entity_id = oci_core_service_gateway.sgw.id
  #   description       = "All Services in region to Service Gateway"
  # }

  freeform_tags = var.freeform_tags
  defined_tags  = var.defined_tags

  lifecycle {
    ignore_changes = [defined_tags, freeform_tags]
  }
}

# Service Gateway (SGW) - routing to oracle service network
# data "oci_core_services" "all_oci_services" {
#   filter {
#     name   = "name"
#     values = ["All .* Services In Oracle Services Network"]
#     regex  = true
#   }
# }
#
# resource "oci_core_service_gateway" "sgw" {
#   compartment_id = var.compartment_id
#   vcn_id         = oci_core_vcn.this.id
#   display_name   = "${var.prefix}-service-gateway"
#
#   services {
#     service_id = lookup(data.oci_core_services.all_oci_services.services[0], "id")
#   }
#
#   freeform_tags = var.freeform_tags
#   defined_tags  = var.defined_tags
#
#   lifecycle {
#     ignore_changes = [defined_tags, freeform_tags]
#   }
# }
#
# resource "oci_core_route_table" "sgw" {
#   compartment_id = var.compartment_id
#   vcn_id         = oci_core_vcn.this.id
#   display_name   = "${var.prefix}-sgw-route-table"
#
#   route_rules {
#     destination       = lookup(data.oci_core_services.all_oci_services.services[0], "cidr_block")
#     destination_type  = "SERVICE_CIDR_BLOCK"
#     network_entity_id = oci_core_service_gateway.sgw.id
#     description       = "All Services in region to Service Gateway"
#   }
#
#   freeform_tags = var.freeform_tags
#   defined_tags  = var.defined_tags
#
#   lifecycle {
#     ignore_changes = [defined_tags, freeform_tags]
#   }
# }
