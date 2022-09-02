terraform {
  required_providers {
    azurerm = {
      source = "hashicorp/azurerm"
      version = ">= 3.16"
    }
    azapi = {
      source = "azure/azapi"
    }
    databricks = {
      source = "databricks/databricks"
      version = ">= 1.2.1"
    }
    time = {
      source = "hashicorp/time"
    }

  }
}

# TODO Remove dlevy and switch to username from az login
variable "git_credential" {
  description = "Credential to access the nlp-workspace git repository"
  type = string
  sensitive = true
}


locals {
  prefix                    = "dlevy-nlp"
  storage-prefix                    = replace(local.prefix, "-", "0")
}

provider "azurerm" {
  features {}
}

data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "base" {
  name     = "dlevy-anz-nlp"
  location = "Australia East"
}

resource "azapi_resource" "access_connector" {
  type      = "Microsoft.Databricks/accessConnectors@2022-04-01-preview"
  name      = "dlevy-anz-nlp"
  location  = azurerm_resource_group.base.location
  parent_id = azurerm_resource_group.base.id
  identity {
    type = "SystemAssigned"
  }
  body = jsonencode({
    properties = {}
  })
}


resource "azurerm_storage_account" "unity_catalog" {
  name                     = "${local.storage-prefix}0storage"
  resource_group_name      = azurerm_resource_group.base.name
  location                 = azurerm_resource_group.base.location
  tags                     = azurerm_resource_group.base.tags
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true
}

resource "azurerm_storage_container" "unity_catalog" {
  name                  = "metastore"
  storage_account_name  = azurerm_storage_account.unity_catalog.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "landing" {
  name                  = "landing"
  storage_account_name  = azurerm_storage_account.unity_catalog.name
  container_access_type = "private"
}

resource "azurerm_role_assignment" "uc-role-storage" {
  scope                = azurerm_storage_account.unity_catalog.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azapi_resource.access_connector.identity[0].principal_id
}

resource "azurerm_databricks_workspace" "workspace" {
  name                = "dlevy-anz-nlp"
  resource_group_name = azurerm_resource_group.base.name
  location            = azurerm_resource_group.base.location
  sku                 = "premium"

  tags = {
    Owner = "dlevy@databricks.com"
    Environment = "Dev"
  }
}

// Print the URL to the job.
output "job_url" {
  value = azurerm_databricks_workspace.workspace.workspace_url
}


provider "databricks" {
  host = "https://${azurerm_databricks_workspace.workspace.workspace_url}/"
}

resource "databricks_metastore" "this" {
  name = "dlevy-nlp"
  storage_root = format("abfss://%s@%s.dfs.core.windows.net/",
    azurerm_storage_container.unity_catalog.name,
  azurerm_storage_account.unity_catalog.name)
  force_destroy = true
}

resource "databricks_metastore_data_access" "first" {
  metastore_id = databricks_metastore.this.id
  name         = "dlevy-mi-meta"
  azure_managed_identity {
    access_connector_id = azapi_resource.access_connector.id
  }
  is_default = true
}

resource "databricks_metastore_assignment" "this" {
  workspace_id         = azurerm_databricks_workspace.workspace.workspace_id
  metastore_id         = databricks_metastore.this.id
  default_catalog_name = "hive_metastore"
}

resource "databricks_storage_credential" "external_mi" {
  name = "external_locs"
  azure_managed_identity {
    access_connector_id = azapi_resource.access_connector.id
  }
  comment = "Managed identity credential managed by TF"
  depends_on = [databricks_metastore_assignment.this]
}

resource "databricks_external_location" "landing" {
  name = "landing"
  url = format("abfss://%s@%s.dfs.core.windows.net/",
  azurerm_storage_container.landing.name,
  azurerm_storage_account.unity_catalog.name
  )
  credential_name = databricks_storage_credential.external_mi.id
  comment         = "Managed by TF"
  depends_on = [
    databricks_metastore_assignment.this
  ]
}

resource "databricks_grants" "data_engineers_storage_cred" {
  storage_credential = databricks_storage_credential.external_mi.id
  grant {
    principal  = "anz pubsec Data Engineers"
    privileges = ["CREATE_TABLE", "READ_FILES", "WRITE_FILES", "CREATE_EXTERNAL_LOCATION"]
  }

}
resource "databricks_grants" "data_engineers_ext_loc_landing" {
  external_location = databricks_external_location.landing.id
  grant {
    principal  = "anz pubsec Data Engineers"
    privileges = ["READ_FILES", "WRITE_FILES"]
  }
}

# TODO switch username to variable.
resource "databricks_git_credential" "ado" {
  git_username          = "davidglevy"
  git_provider          = "gitHub"
  personal_access_token = var.git_credential
}

resource "databricks_repo" "data-engineering" {
  url = "https://github.com/davidglevy/nlp-workspace"
}

data "databricks_node_type" "smallest" {
  local_disk = true
  min_cores   = 16
  gb_per_core = 1
  depends_on = [azurerm_databricks_workspace.workspace]
}

data "databricks_spark_version" "latest_lts" {
  long_term_support = true
  depends_on = [azurerm_databricks_workspace.workspace]
}

resource "databricks_cluster" "shared_autoscaling" {
  cluster_name            = "Shared Autoscaling"
  spark_version           = data.databricks_spark_version.latest_lts.id
  node_type_id            = "Standard_D8ds_v4"
  data_security_mode      = "SINGLE_USER"
  autotermination_minutes = 20
  autoscale {
    min_workers = 2
    max_workers = 8
  }
}

data "databricks_current_user" "me" {
  depends_on = [azurerm_databricks_workspace.workspace]
}

/*
// Add DLT pipeline.
resource "databricks_pipeline" "example" {
  name    = "Example Pipeline"
  storage = "/tmp/${data.databricks_current_user.me.alphanumeric}/dlt_pipeline"
  configuration = {
    "mypipeline.data_path" = "${data.databricks_current_user.me.alphanumeric}"
  }
  target = "${data.databricks_current_user.me.alphanumeric}_ap_juice_db_dlt"
  development = true
  continuous = false
  channel = "CURRENT"
  edition = "ADVANCED"
  photon = false
  library {
    notebook {
      path = "/Repos/${data.databricks_current_user.me.user_name}/apjbootcamp2022/Lab 01 - Data Engineering/04 - Delta Live Tables (SQL)"
    }
  }

}

// Add Job.
// TODO Update DLT task with id
// TODO Add job tags for searching

resource "databricks_job" "example_repos" {
  name = "Example Using Repos"

  task {
    task_key = "01_Delta_Tables"
    notebook_task {
      notebook_path = "/Repos/${data.databricks_current_user.me.user_name}/apjbootcamp2022/Lab 01 - Data Engineering/01 - Delta Tables"
    }
    existing_cluster_id = databricks_cluster.shared_autoscaling.id
  }

  task {
    task_key = "02_ELT"
    notebook_task {
      notebook_path = "/Repos/${data.databricks_current_user.me.user_name}/apjbootcamp2022/Lab 01 - Data Engineering/02 - ELT"
    }
    existing_cluster_id = databricks_cluster.shared_autoscaling.id
    depends_on {
      task_key = "01_Delta_Tables"
    }
  }

  task {
    task_key = "03_DLT"
    pipeline_task {
      pipeline_id = databricks_pipeline.example.id
    }
    depends_on {
      task_key = "02_ELT"
    }
  }


}

resource "databricks_job" "example_git" {
  name = "Example Using Git"

  // Example of a git sourced job
  git_source {
    url = "https://github.com/zivilenorkunaite/apjbootcamp2022"
    branch = "main"
    provider = "github"
  }


  task {
    task_key = "01_Delta_Tables"
    notebook_task {
      notebook_path = "Lab 01 - Data Engineering/01 - Delta Tables"
    }
    existing_cluster_id = databricks_cluster.shared_autoscaling.id
  }

  task {
    task_key = "02_ELT"
    notebook_task {
      notebook_path = "Lab 01 - Data Engineering/02 - ELT"
    }
    existing_cluster_id = databricks_cluster.shared_autoscaling.id
    depends_on {
      task_key = "01_Delta_Tables"
    }
  }

}
*/