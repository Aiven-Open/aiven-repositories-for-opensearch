# Aiven OpenSearch Repository Plugins

This repository provides Aiven's custom OpenSearch repository plugins for:
- **S3** (`repository-s3`)
- **Google Cloud Storage (GCS)** (`repository-gcs`)
- **Azure Blob Storage** (`repository-azure`)

These plugins are needed to enable secure, cloud-native snapshot and restore for OpenSearch clusters, with support for encryption and advanced configuration.

**Key differences from the built-in OpenSearch plugins:**
- Support for multiple OpenSearch versions at once (2.17.x and higher).
- Enhanced security features (encryption, key management).
- Additional configuration options and cloud compatibility improvements.

Use these plugins if you need more flexibility, security, or multi-version support than the default OpenSearch repository plugins provide.