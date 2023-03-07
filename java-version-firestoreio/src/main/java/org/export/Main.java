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
 * ject=PROJECT_NAME --tempLocation=gs://BUCKET_NAME/temp/dataflow" -Pdirect-runner
 * --input=FILE_PATH_ON_BUCKET
 */
public class Main {
    public static void main(String[] args) {

        PipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);
        RpcQosOptions rpcQosOptions =
                RpcQosOptions.newBuilder()
                        .withHintMaxNumWorkers(options.as(DataflowPipelineOptions.class).getMaxNumWorkers())
                        .build();

        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("Read from GCS", TextIO.read().from(options.getInput()));
                // Maps records to strings
                .apply(MapElements.via(
                    new SimpleFunction<String, Write>() {
                        @Override
                        public Write apply(String record) {
                            return Write.newBuilder()
                                    .setUpdate(
                                            Document.newBuilder()
                                                    .putFields("name", Value.newBuilder().setStringValue("Tokyo").build())
                                                    .putFields("country", Value.newBuilder().setStringValue("Japan").build())
                                                    .putFields("capital", Value.newBuilder().setBooleanValue(true).build()))
                                    .build();
                        }
                })
                // batch write the data
                .apply(FirestoreIO.v1().write()
                        .batchWrite().withRpcQosOptions(rpcQosOptions).build());

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