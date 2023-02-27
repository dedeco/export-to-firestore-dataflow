import argparse
import json
import logging
from typing import List

import apache_beam as beam
from apache_beam.options.pipeline_options import SetupOptions, PipelineOptions
from google.cloud import firestore


class ReadJson(beam.PTransform):

    def __init__(self, file_pattern):
        self._file_pattern = file_pattern

    def expand(self, pcoll):
        return (pcoll.pipeline
                | beam.io.ReadFromText(self._file_pattern)
                | beam.Map(json.loads))


class Transformation:

    def __init__(self):
        pass

    @staticmethod
    def remap(json_as_dict: dict) -> List[dict]:
        user = {
            "us_cpf": json_as_dict.get('cpf'),
            "us_gender": json_as_dict.get('genero'),
            "us_name": json_as_dict.get('nome')
        }
        return user


class FirestoreUpdateDoFn(beam.DoFn):

    def __init__(self, max_batch_size=500):
        self.element_batch = []
        self.max_batch_size = max_batch_size

    def start_bundle(self):
        self.db = firestore.Client(project='andresousa-dataform-dev')
        self.batch = self.db.batch()
        self.some_ref = self.db.collection(u'Users')

    def process(self, row):
        self.element_batch.append(row)
        if len(self.element_batch) >= self.max_batch_size:
            self._flush_updates()

    def finish_bundle(self):
        self._flush_updates()
        self.db.close()

    def _flush_updates(self):
        for item in self.element_batch:
            doc = self.some_ref.document()
            doc.set(
                item
            )
        self.batch.commit()


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the export firestore pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://andresousa-dataform-devcs-0/2023/02/dump-bq-users*.json',
        help='Input file partner to process.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = save_main_session

    transform = Transformation()

    with beam.Pipeline(options=options) as p:
        (
                p
                | "read" >> ReadJson(known_args.input)
                | "remap to a new json" >> beam.Map(lambda json_as_dict: transform.remap(json_as_dict))
                | "write in firestore" >> beam.io.Write(
            beam.ParDo(FirestoreUpdateDoFn())
        )
        )
    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
