




docker run -d --name fake-gcs-server -p 4443:4443 fsouza/fake-gcs-server

docker run -d --name fake-gcs-server -p 4443:4443 -e GCS_BUCKETS=test-bucket fsouza/fake-gcs-server



STORAGE_EMULATOR_HOST=


set STORAGE_EMULATOR_HOST=localhost:4443

failed to start emulator: listen tcp: address ::1:8688: too many colons in address

set STORAGE_EMULATOR_HOST=localhost:4443
gcloud beta emulators bigquery start --project


docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' 7910e54b6a28

