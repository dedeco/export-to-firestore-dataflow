# Goal
Dataflow approach using apache beam to export json to firestore

## How to run

1. Copy input-samples/*.json to some Cloud Storage
2. Install requirements
```bash
pip install 'apache-beam[gcp]'
pip install -r requirements.txt
```

4. Submit the command (to run locally) (Optional):
```bash
python -m export_firestore_pipeline --input "gs://andresousa-dataform-devcs-0/2023/02/dump-bq-users*.json" --project "andresousa-dataform-dev" --project_firestore_host "andresousa-dataform-dev" --region southamerica-east1 --temp_location "gs://andresousa-dataform-devcs-0/tmp/" --requirements_file "requirements.txt"
```
5. Submit the command to RUN IN DATAFLOW:
```bash
python -m export_firestore_pipeline --input "gs://andresousa-dataform-devcs-0/2023/02/dump-bq-users*.json" --runner "DataflowRunner" --project "andresousa-dataform-dev" --project_firestore_host "andresousa-dataform-dev" --region southamerica-east1 --temp_location "gs://andresousa-dataform-devcs-0/tmp/" --requirements_file "requirements.txt"
```

## Disclaimer
Firestore io Connector is not supported as you follow: https://beam.apache.org/documentation/io/connectors/. This approach consume Firestore API directly, so adjusts a throttle is some errors occurring. 