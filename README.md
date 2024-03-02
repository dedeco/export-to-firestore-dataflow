Absolutely! Here's the revised README.md formatted in Markdown for a clean and easily readable presentation:

**export_firestore_pipeline**

## Goal

This Apache Beam pipeline provides a solution to export JSON data into a Google Cloud Firestore database. It performs the following:

* **Reads JSON Data:** Reads JSON files from a specified location.
* **Transforms JSON Structure:** Remaps the JSON data to a format suitable for Firestore.
* **Writes to Firestore:** Inserts the transformed data into Firestore documents.

## How to Run

**Prerequisites:**

* A Google Cloud Project with a Firestore database.
* A Google Cloud Storage bucket to store sample JSON files.

**Steps:**

1. **Upload JSON Files:** Copy your sample JSON files (`input-samples/*.json`) to your Cloud Storage bucket.

2. **Install Dependencies:**

   ```bash
   pip install 'apache-beam[gcp]'
   pip install -r requirements.txt 
   ```

3. **Execute the Pipeline:**

   **To run locally (optional):**

   ```bash
   python -m export_firestore_pipeline \
       --input "gs://your-bucket-name/path/to/json/files*.json" \
       --project "your-gcp-project-id" \
       --project_firestore_host "your-gcp-project-id" \
       --region southamerica-east1 \
       --temp_location "gs://your-bucket-name/tmp/" \
       --requirements_file "requirements.txt"
   ```

   **To run on Google Cloud Dataflow:**

   ```bash
   python -m export_firestore_pipeline \
       --input "gs://your-bucket-name/path/to/json/files*.json" \
       --runner "DataflowRunner" \
       --project "your-gcp-project-id" \
       --project_firestore_host "your-gcp-project-id" \
       --region southamerica-east1 \
       --temp_location "gs://your-bucket-name/tmp/" \
       --requirements_file "requirements.txt"
   ```

   **Replace placeholders like `your-bucket-name` and `your-gcp-project-id` with your actual values.**

## Disclaimer

The Firestore IO connector is not officially supported by Apache Beam. This pipeline interacts directly with the Firestore API. Please be aware of potential throttling limits, and adjust your pipeline usage accordingly if errors occur. 
