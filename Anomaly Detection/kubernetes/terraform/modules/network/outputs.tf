output "vcn_id" {
  description = "id of vcn created"
  value       = oci_core_vcn.this.id
}

output "internet_gateway_id" {
  description = "id of internet gateway created"
  value       = oci_core_internet_gateway.igw.id
}

output "nat_gateway_id" {
  description = "id of NAT gateway created"
  value       = oci_core_nat_gateway.ngw.id
}

# output "service_gateway_id" {
#   description = "id of service gateway created"
#   value       = oci_core_service_gateway.sgw.id
# }

output "private_subnet_id" {
  description = "id of private subnet created"
  value       = oci_core_subnet.private.id
}

output "public_subnet_id" {
  description = "id of public subnet created"
  value       = oci_core_subnet.public.id
}