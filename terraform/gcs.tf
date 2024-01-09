# Create new storage bucket in the US multi-region
# with standard storage

resource "google_storage_bucket" "static" {
 name          = "${var.BUCKET_NAME}"
 location      = "${var.Region}"
 storage_class = "${var.Storage}"

 uniform_bucket_level_access = true
}

# Upload a text file as an object
# to the storage bucket

resource "google_storage_bucket_object" "default" {
 name         = "${var.OBJECT_NAME}"
 source       = "${var.OBJECT_PATH}"
 content_type = "${var.Content}"
 bucket       = google_storage_bucket.static.id
}