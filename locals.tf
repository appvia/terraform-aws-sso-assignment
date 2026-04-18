locals {
  ## The current AWS region
  region = data.aws_region.current.region
  ## Common tags applied to all resources, merged with user-provided tags
  tags = merge(
    var.tags,
    {
      Provisioner = "Terraform"
    }
  )
}
