terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
      version = ">= 1.2.1"
    }
  }
}
  # TODO Remove dlevy and switch to username from az login
variable "workspace_url" {
  description = "The workspace URL"
  type = string
  sensitive = true
}
provider "databricks" {
  host = var.workspace_url
}

data "databricks_spark_version" "latest_lts" {
  long_term_support = true
}

resource "databricks_cluster" "shared_autoscaling" {
  cluster_name            = "NLP Cluster 2"
  spark_version           = data.databricks_spark_version.latest_lts.id
  node_type_id            = "Standard_D8ds_v4"
  data_security_mode      = "SINGLE_USER"
  single_user_name        = "dlevy@databricks.com"
  autotermination_minutes = 60

  // Add libraries
  // TODO Make Wheel from deps
  library {
    pypi {
      package = "pdf2image"
    }

  }
  library {
    pypi {
      package = "git+https://github.com/facebookresearch/detectron2.git@v0.5#egg=detectron2"

    }
  }
  library {
    pypi {
      package = "layoutparser[ocr]"
    }
  }
  library {
    pypi {
      package = "torchvision"
    }
  }

  // Add init script
  init_scripts {
    dbfs {
      destination = "dbfs:/scripts/cluster_init.sh"
    }
  }

  // Set number of workers (2)
  num_workers = 2

}
