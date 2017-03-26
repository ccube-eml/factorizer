import os
import tempfile
import unittest

from factorizer.data_drivers.postgresql_data_driver import PostgreSQLDataDriver


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

TRAINING_RATE = 0.5
FUSION_RATE = 0.3
TRAINING_SAMPLE_RATE = 0.1
TRAINING_SAMPLE_NUMBER = 1
CLASS_ATTRIBUTE = 'label'
INCLUDE_ATTRIBUTES = []
EXCLUDE_ATTRIBUTES = []
ATTRIBUTES_RATE = 0.5
RANDOM_SEED = 0

INCLUDE_HEADER = False

POSTGRESQL_HOSTNAME = 'localhost'
POSTGRESQL_PORT = '5432'
POSTGRESQL_DATABASE = 'postgres'
POSTGRESQL_USERNAME = 'postgres'
POSTGRESQL_PASSWORD = 'postgres'


class PostgreSQLDataDriverTest(unittest.TestCase):
    def setUp(self):
        self.__postgresql_data_driver = PostgreSQLDataDriver(
            POSTGRESQL_DATABASE,
            POSTGRESQL_USERNAME,
            POSTGRESQL_PASSWORD,
            POSTGRESQL_HOSTNAME,
            POSTGRESQL_PORT
        )

    def tearDown(self):
        self.__postgresql_data_driver.close()

    def test_structure_creation(self):
        self.__postgresql_data_driver.destroy_structure(
            name=DATASET_NAME,
        )

        self.__postgresql_data_driver.create_structure(
            name=DATASET_NAME,
            attributes=DATASET_ATTRIBUTES,
        )

        self.__postgresql_data_driver.destroy_structure(
            name=DATASET_NAME,
        )

    def test_fill_structure(self):
        self.__postgresql_data_driver.destroy_structure(
            name=DATASET_NAME,
        )

        self.__postgresql_data_driver.create_structure(
            name=DATASET_NAME,
            attributes=DATASET_ATTRIBUTES,
        )

        with open(DATASET_FILE_PATH, mode='rb') as dataset_file:
            self.__postgresql_data_driver.fill_structure(
                name=DATASET_NAME,
                delimiter=DATASET_DELIMITER,
                header=DATASET_HEADER,
                input_csv=dataset_file,
            )

        self.__postgresql_data_driver.destroy_structure(
            name=DATASET_NAME,
        )

    def test_get_training_split(self):
        self.__postgresql_data_driver.destroy_structure(
            name=DATASET_NAME,
        )

        self.__postgresql_data_driver.create_structure(
            name=DATASET_NAME,
            attributes=DATASET_ATTRIBUTES,
        )

        with open(DATASET_FILE_PATH, mode='rb') as dataset_file:
            self.__postgresql_data_driver.fill_structure(
                name=DATASET_NAME,
                delimiter=DATASET_DELIMITER,
                header=DATASET_HEADER,
                input_csv=dataset_file,
            )

        with tempfile.TemporaryFile() as temporary_file:
            self.__postgresql_data_driver.get_training_split(
                dataset_name=DATASET_NAME,
                training_rate=TRAINING_RATE,
                class_attribute=CLASS_ATTRIBUTE,
                include_attributes=INCLUDE_ATTRIBUTES,
                exclude_attributes=EXCLUDE_ATTRIBUTES,
                attributes_rate=ATTRIBUTES_RATE,
                random_seed=RANDOM_SEED,
                output_csv=temporary_file,
                include_header=INCLUDE_HEADER,
                class_only=False,
            )

            temporary_file.seek(0)

            for line in temporary_file:
                print(line)

        self.__postgresql_data_driver.destroy_structure(
            name=DATASET_NAME,
        )

    def test_get_fusion_split(self):
        self.__postgresql_data_driver.destroy_structure(
            name=DATASET_NAME,
        )

        self.__postgresql_data_driver.create_structure(
            name=DATASET_NAME,
            attributes=DATASET_ATTRIBUTES,
        )

        with open(DATASET_FILE_PATH, mode='rb') as dataset_file:
            self.__postgresql_data_driver.fill_structure(
                name=DATASET_NAME,
                delimiter=DATASET_DELIMITER,
                header=DATASET_HEADER,
                input_csv=dataset_file,
            )

        with tempfile.TemporaryFile() as temporary_file:
            self.__postgresql_data_driver.get_fusion_split(
                dataset_name=DATASET_NAME,
                training_rate=TRAINING_RATE,
                fusion_rate=FUSION_RATE,
                class_attribute=CLASS_ATTRIBUTE,
                include_attributes=INCLUDE_ATTRIBUTES,
                exclude_attributes=EXCLUDE_ATTRIBUTES,
                attributes_rate=ATTRIBUTES_RATE,
                random_seed=RANDOM_SEED,
                output_csv=temporary_file,
                include_header=INCLUDE_HEADER,
                class_only=False,
            )

            temporary_file.seek(0)

            for line in temporary_file:
                print(line)

        self.__postgresql_data_driver.destroy_structure(
            name=DATASET_NAME,
        )

    def test_get_test_split(self):
        self.__postgresql_data_driver.destroy_structure(
            name=DATASET_NAME,
        )

        self.__postgresql_data_driver.create_structure(
            name=DATASET_NAME,
            attributes=DATASET_ATTRIBUTES,
        )

        with open(DATASET_FILE_PATH, mode='rb') as dataset_file:
            self.__postgresql_data_driver.fill_structure(
                name=DATASET_NAME,
                delimiter=DATASET_DELIMITER,
                header=DATASET_HEADER,
                input_csv=dataset_file,
            )

        with tempfile.TemporaryFile() as temporary_file:
            self.__postgresql_data_driver.get_test_split(
                dataset_name=DATASET_NAME,
                training_rate=TRAINING_RATE,
                fusion_rate=FUSION_RATE,
                class_attribute=CLASS_ATTRIBUTE,
                include_attributes=INCLUDE_ATTRIBUTES,
                exclude_attributes=EXCLUDE_ATTRIBUTES,
                attributes_rate=ATTRIBUTES_RATE,
                random_seed=RANDOM_SEED,
                output_csv=temporary_file,
                include_header=INCLUDE_HEADER,
                class_only=False,
            )

            temporary_file.seek(0)

            for line in temporary_file:
                print(line)

        self.__postgresql_data_driver.destroy_structure(
            name=DATASET_NAME,
        )

    def test_get_training_sample(self):
        self.__postgresql_data_driver.destroy_structure(
            name=DATASET_NAME,
        )

        self.__postgresql_data_driver.create_structure(
            name=DATASET_NAME,
            attributes=DATASET_ATTRIBUTES,
        )

        with open(DATASET_FILE_PATH, mode='rb') as dataset_file:
            self.__postgresql_data_driver.fill_structure(
                name=DATASET_NAME,
                delimiter=DATASET_DELIMITER,
                header=DATASET_HEADER,
                input_csv=dataset_file,
            )

        with tempfile.TemporaryFile() as temporary_file:
            self.__postgresql_data_driver.get_training_sample(
                dataset_name=DATASET_NAME,
                training_rate=TRAINING_RATE,
                sample_rate=TRAINING_SAMPLE_RATE,
                sample_number=TRAINING_SAMPLE_NUMBER,
                class_attribute=CLASS_ATTRIBUTE,
                include_attributes=INCLUDE_ATTRIBUTES,
                exclude_attributes=EXCLUDE_ATTRIBUTES,
                attributes_rate=ATTRIBUTES_RATE,
                random_seed=RANDOM_SEED,
                output_csv=temporary_file,
                include_header=INCLUDE_HEADER,
                class_only=False,
            )

            temporary_file.seek(0)

            for line in temporary_file:
                print(line)

        self.__postgresql_data_driver.destroy_structure(
            name=DATASET_NAME,
        )

    def test_get_fusion_split_class(self):
        self.__postgresql_data_driver.destroy_structure(
            name=DATASET_NAME,
        )

        self.__postgresql_data_driver.create_structure(
            name=DATASET_NAME,
            attributes=DATASET_ATTRIBUTES,
        )

        with open(DATASET_FILE_PATH, mode='rb') as dataset_file:
            self.__postgresql_data_driver.fill_structure(
                name=DATASET_NAME,
                delimiter=DATASET_DELIMITER,
                header=DATASET_HEADER,
                input_csv=dataset_file,
            )

        with tempfile.TemporaryFile() as temporary_file:
            self.__postgresql_data_driver.get_fusion_split(
                dataset_name=DATASET_NAME,
                training_rate=TRAINING_RATE,
                fusion_rate=FUSION_RATE,
                class_attribute=CLASS_ATTRIBUTE,
                include_attributes=INCLUDE_ATTRIBUTES,
                exclude_attributes=EXCLUDE_ATTRIBUTES,
                attributes_rate=ATTRIBUTES_RATE,
                random_seed=RANDOM_SEED,
                output_csv=temporary_file,
                include_header=INCLUDE_HEADER,
                class_only=True,
            )

            temporary_file.seek(0)

            for line in temporary_file:
                print(line)

        self.__postgresql_data_driver.destroy_structure(
            name=DATASET_NAME,
        )
