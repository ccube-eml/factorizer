import os
import json
import threading
import time

import requests

from factorizer.__main__ import app

THIS_DIRECTORY_PATH = os.path.dirname(os.path.abspath(__file__))

DATASET_FILE_PATH = os.path.join(THIS_DIRECTORY_PATH, 'resources/datasets/higgs-1000.csv')
DATASET_NAME = 'higgs'
DATASET_DELIMITER = ','
DATASET_HEADER = False
DATASET_ATTRIBUTES = [
    {
        'name': 'label',
        'type': 'real',
    },
    {
        'name': 'lepton_pt',
        'type': 'real',
    },
    {
        'name': 'lepton_eta',
        'type': 'real',
    },
    {
        'name': 'lepton_phi',
        'type': 'real',
    },
    {
        'name': 'missing_energy_magnitude',
        'type': 'real',
    },
    {
        'name': 'missing_energy_phi',
        'type': 'real',
    },
    {
        'name': 'jet_1_pt',
        'type': 'real',
    },
    {
        'name': 'jet_1_eta',
        'type': 'real',
    },
    {
        'name': 'jet_1_phi',
        'type': 'real',
    },
    {
        'name': 'jet_1_b-tag',
        'type': 'real',
    },
    {
        'name': 'jet_2_pt',
        'type': 'real',
    },
    {
        'name': 'jet_2_eta',
        'type': 'real',
    },
    {
        'name': 'jet_2_phi',
        'type': 'real',
    },
    {
        'name': 'jet_2_b-tag',
        'type': 'real',
    },
    {
        'name': 'jet_3_pt',
        'type': 'real',
    },
    {
        'name': 'jet_3_eta',
        'type': 'real',
    },
    {
        'name': 'jet_3_phi',
        'type': 'real',
    },
    {
        'name': 'jet_3_b-tag',
        'type': 'real',
    },
    {
        'name': 'jet_4_pt',
        'type': 'real',
    },
    {
        'name': 'jet_4_eta',
        'type': 'real',
    },
    {
        'name': 'jet_4_phi',
        'type': 'real',
    },
    {
        'name': 'jet_4_b-tag',
        'type': 'real',
    },
    {
        'name': 'm_jj',
        'type': 'real',
    },
    {
        'name': 'm_jjj',
        'type': 'real',
    },
    {
        'name': 'm_lv',
        'type': 'real',
    },
    {
        'name': 'm_jlv',
        'type': 'real',
    },
    {
        'name': 'm_bb',
        'type': 'real',
    },
    {
        'name': 'm_wbb',
        'type': 'real',
    },
    {
        'name': 'm_wwbb',
        'type': 'real',
    },
]

POST_DATASET_REQUEST_URL = 'http://{hostname_}:{port_}/dataset'
POSTGRESQL_HOSTNAME = 'localhost'
FACTORIZER_HOSTNAME = 'localhost'
FACTORIZER_PORT = '5000'


if __name__ == '__main__':
    os.environ['POSTGRESQL_HOSTNAME'] = POSTGRESQL_HOSTNAME

    # Launches the daemon.
    def launch_daemon():
        app.run(host='0.0.0.0', debug=False)
    daemon_thread = threading.Thread(target=launch_daemon)
    daemon_thread.start()

    time.sleep(2)

    # Creates and fills the structure.
    with open(DATASET_FILE_PATH, mode='rb') as dataset_file:
        request_data = {
            'name': DATASET_NAME,
            'attributes': json.dumps(DATASET_ATTRIBUTES),
            'delimiter': DATASET_DELIMITER,
            'header': DATASET_HEADER,
        }

        request_files = {
            'dataset': dataset_file,
        }

        response = requests.post(
            POST_DATASET_REQUEST_URL.format(
                hostname_=FACTORIZER_HOSTNAME,
                port_=FACTORIZER_PORT,
            ),
            data=request_data,
            files=request_files,
        )
