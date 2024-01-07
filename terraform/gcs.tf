# Create new storage bucket in the US multi-region
# with standard storage
# test

resource "google_storage_bucket" "static" {
 name          = ${var.BUCKET_NAME}
 location      = ${var.US}
 storage_class = ${var.STANDARD}

 uniform_bucket_level_access = true
}

# Upload a text file as an object
# to the storage bucket

resource "google_storage_bucket_object" "default" {
 name         = ${var.OBJECT_NAME}
 source       = ${var.OBJECT_PATH}
 content_type = ${var.text/plain}
 bucket       = google_storage_bucket.static.id
}
