resource "oci_core_default_security_list" "default" {
  manage_default_resource_id = oci_core_vcn.this.default_security_list_id

  egress_security_rules { #ipv4 - outbound traffic
    destination = local.anywhere
    protocol    = "all"
    description = "allow all egress traffic"
  }

  dynamic "egress_security_rules" { #ipv6 - outbound traffic
    for_each = var.enable_ipv6 == true ? [1] : []

    content {
      destination = local.anywhere_ipv6
      protocol    = "all"
      description = "allow all egress traffic IPv6"
    }
  }

  ingress_security_rules { #ipv4 - ssh
    protocol    = "6"
    source      = local.anywhere
    description = "allow all SSH ingress"
    tcp_options {
      min = 22
      max = 22
    }
  }

  dynamic "ingress_security_rules" { #ipv6 - ssh
    for_each = var.enable_ipv6 == true ? [1] : []

    content {
      protocol    = "6"
      source      = local.anywhere_ipv6
      description = "allow all SSH ingress IPv6"
      tcp_options {
        min = 22
        max = 22
      }
    }
  }

  ingress_security_rules { #ipv4 - icmp
    protocol    = "1"
    source      = local.anywhere
    description = "allow ICMP for all type 3 code 4"
    icmp_options {
      type = "3"
      code = "4"
    }
  }

  dynamic "ingress_security_rules" { #ipv6 - icmp
    for_each = var.enable_ipv6 == true ? [1] : []

    content {
      protocol    = "1"
      source      = local.anywhere_ipv6
      description = "allow ICMP for all type 3 code 4 IPv6"
      icmp_options {
        type = "3"
        code = "4"
      }
    }
  }

  ingress_security_rules { # local icmp
    protocol = "1"
    source   = var.vcn_cidr
    icmp_options {
      type = "3"
    }
    description = "allow all ICMP from VCN"
  }

  freeform_tags = var.freeform_tags
  defined_tags  = var.defined_tags

  lifecycle {
    ignore_changes = [egress_security_rules, ingress_security_rules, freeform_tags, defined_tags]
  }
}