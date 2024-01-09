variable "BUCKET_NAME"{
  type=string
  default="dev"
}

variable "Region"{
  type=string
  default="US"
}

variable "Storage"{
  type=string
  default="STANDARD"
}

variable "OBJECT_NAME"{
  type=string
  default="gcs_tests"
}

variable "OBJECT_PATH"{
  type=string
  default="root/modules/"
}

variable "Content"{
  type=string
  default="text/plain"
}

