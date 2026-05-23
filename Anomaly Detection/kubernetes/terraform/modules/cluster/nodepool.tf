data "oci_identity_availability_domains" "ads" {
  compartment_id = var.compartment_id
}

data "oci_core_images" "latest_image" {
  compartment_id           = var.compartment_id
  operating_system         = "Oracle Linux"
  operating_system_version = "8"
  filter {
    name   = "display_name"
    values = ["^.*-aarch64-.*$"]
    regex  = true
  }
}

resource "oci_containerengine_node_pool" "this" {
  compartment_id     = var.compartment_id
  cluster_id         = oci_containerengine_cluster.this.id
  kubernetes_version = var.kubernetes_version
  name               = "${var.prefix}-node-pool"

  node_config_details {
    dynamic "placement_configs" {
      for_each = data.oci_identity_availability_domains.ads.availability_domains
      content {
        availability_domain = placement_configs.value.name
        subnet_id           = data.oci_core_subnet.private.id
      }
    }
    size = var.node_pool_size
  }
  node_shape = var.node_shape
  node_shape_config {
    memory_in_gbs = var.node_shape_config.memory_in_gbs
    ocpus         = var.node_shape_config.ocpus
  }

  node_source_details {
    image_id    = data.oci_core_images.latest_image.images[0].id
    source_type = "image"

    boot_volume_size_in_gbs = var.shape_boot_volume_size
  }

  initial_node_labels {
    key   = "name"
    value = "${var.prefix}-cluster"
  }

  ssh_public_key = var.ssh_public_key

  freeform_tags = var.freeform_tags
  defined_tags  = var.defined_tags

  lifecycle {
    ignore_changes = [defined_tags, freeform_tags]
  }
}

# write kube config file
data "oci_containerengine_cluster_kube_config" "k8s_cluster_kube_config" {
  cluster_id = oci_containerengine_cluster.this.id
}

resource "local_file" "kube_config" {
  depends_on      = [oci_containerengine_node_pool.this]
  content         = data.oci_containerengine_cluster_kube_config.k8s_cluster_kube_config.content
  filename        = "../.kube.config"
  file_permission = 0400
}