#######################################################################################################################
#### Databricks Clusters & Libraries
#######################################################################################################################
## Create databricks_cluster
resource "databricks_cluster" "databricks_cluster" {
  cluster_name            = var.databricks_cluster_name
  spark_version           = var.databricks_spark_version
  node_type_id            = var.databricks_node_type_id
  autotermination_minutes = var.databricks_autotermination_minutes
  autoscale {
              min_workers = var.databricks_min_workers
              max_workers = var.databricks_max_workers
  }
  
  ## Create databricks_libraries
  library {
    pypi {
        package = "pyodbc"
        }
  }
   library {
    pypi {
        package = "requests"
        }
  }
   library {
    pypi {
        package = "applicationinsights"
        }
  }
   library {
    pypi {
        package = "ff3"
        }
  }
   library {
    pypi {
        package = "pycryptodome"
        }
  }
     library {
    pypi {
        package = "great_expectations"
        }
  }
  library {
    maven {
      coordinates = "com.microsoft.azure:applicationinsights-core_2.6_3"
    }
  }
  library {
    maven {
      coordinates = "org.cryptomator:siv-mode:1.4.3"
    }
  }
  custom_tags = {
    Department = "Data Engineering"
  }

  depends_on = [ azurerm_databricks_workspace.databricks_workspace ]
}

#######################################################################################################################
#### Add to Key Vault
#######################################################################################################################
## Add Key Vault Secret: Databricks Cluster-01 ID
resource "azurerm_key_vault_secret" "kv_dbx_cluster_01_id" {
  name         = "dbx-cluster-01-id"
  value        = databricks_cluster.databricks_cluster.id
  key_vault_id = var.key_vault_id
  content_type = "Databricks Cluster-01 ID"
  depends_on   = [ databricks_cluster.databricks_cluster ]
}