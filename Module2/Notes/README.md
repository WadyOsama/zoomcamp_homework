# ETL Pipelines in Kestra: Google Cloud Platform
To reduce unnecessary wait times during each test run, implement a task that verifies the file's existence in the GCS bucket before attempting an upload. This will prevent redundant downloads and uploads and improve efficiency.

```yaml
- id: list_files_in_gcs
  type: io.kestra.plugin.gcp.gcs.List
  filter: FILES
  from: "{{render(vars.gcs_file)}}"
  regExp: "{{render(vars.gcs_file)}}"
```
Then add this property to both **extract** & **upload_to_gcs** tasks:

```yaml
runIf: "{{ outputs.list_files_in_gcs.blobs | length == 0 }}"
```