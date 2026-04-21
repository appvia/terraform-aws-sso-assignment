locals {
  ## Common tags applied to all resources, merged with user-provided tags
  tags = merge(
    var.tags,
    {
      Provisioner = "Terraform"
    }
  )
}
