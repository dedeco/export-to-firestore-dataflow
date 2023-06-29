import argparse
import json
import logging
from typing import List

import apache_beam as beam
from apache_beam.options.pipeline_options import SetupOptions, PipelineOptions


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

    def __init__(self, project_id):
        self.db = None
        self.project_id = project_id

    def start_bundle(self):
        from google.cloud import firestore
        self.db = firestore.Client(self.project_id)

    def process(self, row):
        doc_ref = self.db.collection(u'Users') \
            .document(str(row.get('us_cpf')))
        doc_ref.set(
            row
        )

    def finish_bundle(self):
        self.db.close()


def build_argument_parser(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='Input file partner to process.'
    )
    parser.add_argument(
        '--project_firestore_host',
        dest='project_firestore_host',
        required=True,
        help='Project id that will save firestore data'
    )
    parser.add_argument(
        '--project',
        dest='project',
        required=True,
        help='Project ID.'
    )
    known_args, pipeline_args = parser.parse_known_args(argv)
    logging.info('pipeline_args {}'.format(pipeline_args))
    logging.info('known_args {}'.format(known_args))
    pipeline_args.extend([
        '--project=' + known_args.project
    ])
    return known_args, pipeline_args


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the export firestore pipeline."""
    known_args, pipeline_args = build_argument_parser(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = save_main_session

    transform = Transformation()

    with beam.Pipeline(options=options) as p:
        (
                p
                | "read" >> ReadJson(known_args.input)
                | "remap to a new json" >> beam.Map(lambda json_as_dict: transform.remap(json_as_dict))
                | "write in firestore" >> beam.io.Write(
            beam.ParDo(
                FirestoreUpdateDoFn(known_args.project_firestore_host)
            )
        )
        )
    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
