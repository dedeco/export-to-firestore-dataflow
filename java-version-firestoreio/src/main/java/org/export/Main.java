package org.example;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * mvn compile exec:java -Dexec.mainClass=org.export.Main     -Dexec.args="--pro
 * ject=PROJECT_NAME --tempLocation=gs://BUCKET_NAME/temp/dataflow" -Pdirect-runner --input=
 */
public class Main {
    public static void main(String[] args) {

        PipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("Read from GCS", TextIO.read().from(options.getInput()));

        // Maps records to strings
        mergedTables.apply(MapElements
                .into(TypeDescriptors.strings())
                .via(record -> record.toString())
        )// Writes each window of records into GCS
                .apply(TextIO
                        .write()
                        .to("merged_tables.txt")
                        .withNumShards(1)
                );

        Write write2 =
                Write.newBuilder()
                        .setUpdate(
                                Document.newBuilder()
                                        .putFields("name", Value.newBuilder().setStringValue("Tokyo").build())
                                        .putFields("country", Value.newBuilder().setStringValue("Japan").build())
                                        .putFields("capital", Value.newBuilder().setBooleanValue(true).build()))
                        .build();

        // batch write the data
        pipeline
                .apply(Create.of(write2))
                .apply(FirestoreIO.v1().write().batchWrite().withRpcQosOptions(rpcQosOptions).build());

        // run the pipeline
        pipeline.run().waitUntilFinish();
    }

    public String reformat() {
//        def remap(json_as_dict: dict) -> List[dict]:
//        user = {
//                "us_cpf": json_as_dict.get('cpf'),
//                "us_gender": json_as_dict.get('genero'),
//                "us_name": json_as_dict.get('nome')
//        }
//        return user
    }
}