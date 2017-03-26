import io
import json
import tempfile
import os
import ast

import flask

from factorizer.data_drivers.postgresql_data_driver import PostgreSQLDataDriver


POSTGRESQL_PORT = '5432'
POSTGRESQL_DATABASE = 'postgres'
POSTGRESQL_USERNAME = 'postgres'
POSTGRESQL_PASSWORD = 'postgres'

DATA_CHUNK_SIZE = 4096


# App initialization.
app = flask.Flask(__name__)


def get_data_driver():
    if not hasattr(flask.g, 'data_driver'):
        flask.g.data_driver = PostgreSQLDataDriver(
            POSTGRESQL_DATABASE,
            POSTGRESQL_USERNAME,
            POSTGRESQL_PASSWORD,
            os.environ.get('POSTGRESQL_HOSTNAME', 'postgresql'),
            POSTGRESQL_PORT
        )
    return flask.g.data_driver


@app.teardown_appcontext
def delete_data_driver(exception):
    if hasattr(flask.g, 'data_driver'):
        flask.g.data_driver.close()


@app.route('/dataset', methods=['POST'])
def post_dataset():
    """
    Stores a dataset in a data structure.
    POST: /dataset

    :param form['name']: the name of the structure
    :type form['name']: str

    :param form['attributes']: the json representation of attributes in the form {'name': str, 'type': 'integer' | 'real' | 'text'}
    :type form['attributes']: str

    :param form['delimiter']: the delimiter used in the CSV file (ex. ',')
    :type form['delimiter']: str

    :param form['header']: specifies if the CSV has a header
    :type form['header']: bool

    :param files['dataset']: the file to read
    :type files['dataset']: file
    """
    data_driver = get_data_driver()

    name = flask.request.form.get('name')
    attributes = json.loads(flask.request.form.get('attributes'))
    delimiter = flask.request.form.get('delimiter')
    header = flask.request.form.get('header')

    # Prepares a temporary file before uploading to the database.
    with tempfile.TemporaryFile() as temporary_file:
        # Reads the stream chunk by chunk and writes into the temporary file.
        while True:
            chunk = flask.request.files.get('dataset').stream.read(DATA_CHUNK_SIZE)
            temporary_file.write(chunk)
            if len(chunk) == 0:
                break

        # Shift to the file start.
        temporary_file.seek(0)

        # Destroys the structure.
        data_driver.destroy_structure(
            name=name,
        )

        # Creates the structure.
        data_driver.create_structure(
            name=name,
            attributes=attributes,
        )

        # Fills the structure.
        data_driver.fill_structure(
            name=name,
            delimiter=delimiter,
            header=header,
            input_csv=temporary_file,
        )

    return 'Dataset uploaded correctly.'


@app.route('/dataset/<string:name>', methods=['DELETE'])
def delete_dataset(name):
    """
    Deletes a dataset.
    DELETE: /dataset/<str:name>

    :param name: the name of the dataset
    :type name: str
    """
    data_driver = get_data_driver()

    data_driver.destroy_structure(name=name)

    return 'Dataset deleted correctly.'


@app.route('/dataset/<string:name>/split/training', methods=['GET'])
def get_dataset_training_split(name):
    """
    Retrieves a dataset training split as a CSV file.
    GET: /dataset/<str:name>/split/training

    :param name: the name of the dataset
    :type name: str

    :param args['training_rate']: the percentage of the dataset to consider as training split
    :type args['training_rate']: float

    :param args['class_attribute']: the class attribute name
    :type args['class_attribute']: str

    :param args['include_attributes']: the list of the attributes to include, None otherwise
    :type args['include_attributes']: list[str]

    :param args['exclude_attributes']: the list of the attributes to exclude, None otherwise
    :type args['exclude_attributes']: list[str]

    :param args['attributes_rate']: the percentage of attributes to include
    :type args['attributes_rate']: float

    :param args['random_seed']: the random seed
    :type args['random_seed']: int

    :param args['include_header']: if True, it includes the header in the output CSV
    :type args['include_header']: bool

    :return: the output file as an opened stream
    :rtype: file
    """
    data_driver = get_data_driver()

    training_rate = float(flask.request.args.get('training_rate'))
    class_attribute = flask.request.args.get('class_attribute')
    include_attributes = flask.request.args.getlist('include_attributes')
    exclude_attributes = flask.request.args.getlist('exclude_attributes')
    attributes_rate = float(flask.request.args.get('attributes_rate'))
    random_seed = int(flask.request.args.get('random_seed'))
    include_header = ast.literal_eval(flask.request.args.get('include_header'))

    stream = io.BytesIO()

    data_driver.get_training_split(
        dataset_name=name,
        training_rate=training_rate,
        class_attribute=class_attribute,
        include_attributes=include_attributes,
        exclude_attributes=exclude_attributes,
        attributes_rate=attributes_rate,
        random_seed=random_seed,
        output_csv=stream,
        include_header=include_header,
        class_only=False,
    )

    stream.seek(0)

    return flask.send_file(stream, mimetype='text/csv')


@app.route('/dataset/<string:name>/split/fusion', methods=['GET'])
def get_dataset_fusion_split(name):
    """
    Retrieves a dataset fusion split as a CSV file.
    GET: /dataset/<str:name>/split/fusion

    :param name: the name of the dataset
    :type name: str

    :param args['training_rate']: the percentage of the dataset to consider as training split
    :type args['training_rate']: float

    :param args['fusion_rate']: the percentage of the dataset to consider as fusion split
    :type args['fusion_rate']: float

    :param args['class_attribute']: the class attribute name
    :type args['class_attribute']: str

    :param args['include_attributes']: the list of the attributes to include, None otherwise
    :type args['include_attributes']: list[str]

    :param args['exclude_attributes']: the list of the attributes to exclude, None otherwise
    :type args['exclude_attributes']: list[str]

    :param args['attributes_rate']: the percentage of attributes to include
    :type args['attributes_rate']: float

    :param args['random_seed']: the random seed
    :type args['random_seed']: int

    :param args['include_header']: if True, it includes the header in the output CSV
    :type args['include_header']: bool

    :return: the output file as an opened stream
    :rtype: file
    """
    data_driver = get_data_driver()

    training_rate = float(flask.request.args.get('training_rate'))
    fusion_rate = float(flask.request.args.get('fusion_rate'))
    class_attribute = flask.request.args.get('class_attribute')
    include_attributes = flask.request.args.getlist('include_attributes')
    exclude_attributes = flask.request.args.getlist('exclude_attributes')
    attributes_rate = float(flask.request.args.get('attributes_rate'))
    random_seed = int(flask.request.args.get('random_seed'))
    include_header = ast.literal_eval(flask.request.args.get('include_header'))

    stream = io.BytesIO()

    data_driver.get_fusion_split(
        dataset_name=name,
        training_rate=training_rate,
        fusion_rate=fusion_rate,
        class_attribute=class_attribute,
        include_attributes=include_attributes,
        exclude_attributes=exclude_attributes,
        attributes_rate=attributes_rate,
        random_seed=random_seed,
        output_csv=stream,
        include_header=include_header,
        class_only=False,
    )

    stream.seek(0)

    return flask.send_file(stream, mimetype='text/csv')


@app.route('/dataset/<string:name>/split/test', methods=['GET'])
def get_dataset_test_split(name):
    """
    Retrieves a dataset test split as a CSV file.
    GET: /dataset/<str:name>/split/test

    :param name: the name of the dataset
    :type name: str

    :param args['training_rate']: the percentage of the dataset to consider as training split
    :type args['training_rate']: float

    :param args['fusion_rate']: the percentage of the dataset to consider as fusion split
    :type args['fusion_rate']: float

    :param args['class_attribute']: the class attribute name
    :type args['class_attribute']: str

    :param args['include_attributes']: the list of the attributes to include, None otherwise
    :type args['include_attributes']: list[str]

    :param args['exclude_attributes']: the list of the attributes to exclude, None otherwise
    :type args['exclude_attributes']: list[str]

    :param args['attributes_rate']: the percentage of attributes to include
    :type args['attributes_rate']: float

    :param args['random_seed']: the random seed
    :type args['random_seed']: int

    :param args['include_header']: if True, it includes the header in the output CSV
    :type args['include_header']: bool

    :return: the output file as an opened stream
    :rtype: file
    """
    data_driver = get_data_driver()
    training_rate = float(flask.request.args.get('training_rate'))
    fusion_rate = float(flask.request.args.get('fusion_rate'))
    class_attribute = flask.request.args.get('class_attribute')
    include_attributes = flask.request.args.getlist('include_attributes')
    exclude_attributes = flask.request.args.getlist('exclude_attributes')
    attributes_rate = float(flask.request.args.get('attributes_rate'))
    random_seed = int(flask.request.args.get('random_seed'))
    include_header = ast.literal_eval(flask.request.args.get('include_header'))

    stream = io.BytesIO()

    data_driver.get_test_split(
        dataset_name=name,
        training_rate=training_rate,
        fusion_rate=fusion_rate,
        class_attribute=class_attribute,
        include_attributes=include_attributes,
        exclude_attributes=exclude_attributes,
        attributes_rate=attributes_rate,
        random_seed=random_seed,
        output_csv=stream,
        include_header=include_header,
        class_only=False,
    )

    stream.seek(0)

    return flask.send_file(stream, mimetype='text/csv')


@app.route('/dataset/<string:name>/split/training/sample', methods=['GET'])
def get_dataset_training_sample(name):
    """
    Retrieves a dataset sample within the training split as a CSV file.
    GET: /dataset/<str:name>/split/training/sample

    :param name: the name of the dataset
    :type name: str

    :param args['training_rate']: the percentage of the dataset to consider as training split
    :type args['training_rate']: float

    :param args['sample_rate']: the percentage of instances, within the training split, to include
    :type args['sample_rate']: float

    :param args['sample_number']: the sample number starting from 0
    :type args['sample_number']: int

    :param args['class_attribute']: the class attribute name
    :type args['class_attribute']: str

    :param args['include_attributes']: the list of the attributes to include, None otherwise
    :type args['include_attributes']: list[str]

    :param args['exclude_attributes']: the list of the attributes to exclude, None otherwise
    :type args['exclude_attributes']: list[str]

    :param args['attributes_rate']: the percentage of attributes to include
    :type args['attributes_rate']: float

    :param args['random_seed']: the random seed
    :type args['random_seed']: int

    :param args['include_header']: if True, it includes the header in the output CSV
    :type args['include_header']: bool

    :return: the output file as an opened stream
    :rtype: file
    """
    data_driver = get_data_driver()

    training_rate = float(flask.request.args.get('training_rate'))
    sample_rate = float(flask.request.args.get('sample_rate'))
    sample_number = int(flask.request.args.get('sample_number'))
    class_attribute = flask.request.args.get('class_attribute')
    include_attributes = flask.request.args.getlist('include_attributes')
    exclude_attributes = flask.request.args.getlist('exclude_attributes')
    attributes_rate = float(flask.request.args.get('attributes_rate'))
    random_seed = int(flask.request.args.get('random_seed'))
    include_header = ast.literal_eval(flask.request.args.get('include_header'))

    stream = io.BytesIO()

    data_driver.get_training_sample(
        dataset_name=name,
        training_rate=training_rate,
        sample_rate=sample_rate,
        sample_number=sample_number,
        class_attribute=class_attribute,
        include_attributes=include_attributes,
        exclude_attributes=exclude_attributes,
        attributes_rate=attributes_rate,
        random_seed=random_seed,
        output_csv=stream,
        include_header=include_header,
        class_only=False,
    )

    stream.seek(0)

    return flask.send_file(stream, mimetype='text/csv')


@app.route('/dataset/<string:name>/split/training/class', methods=['GET'])
def get_dataset_training_split_class(name):
    """
    Retrieves a dataset training split as a CSV file.
    GET: /dataset/<str:name>/split/training/class

    :param name: the name of the dataset
    :type name: str

    :param args['training_rate']: the percentage of the dataset to consider as training split
    :type args['training_rate']: float

    :param args['class_attribute']: the class attribute name
    :type args['class_attribute']: str

    :param args['random_seed']: the random seed
    :type args['random_seed']: int

    :param args['include_header']: if True, it includes the header in the output CSV
    :type args['include_header']: bool

    :return: the output file as an opened stream
    :rtype: file
    """
    data_driver = get_data_driver()

    training_rate = float(flask.request.args.get('training_rate'))
    class_attribute = flask.request.args.get('class_attribute')
    random_seed = int(flask.request.args.get('random_seed'))
    include_header = ast.literal_eval(flask.request.args.get('include_header'))

    stream = io.BytesIO()

    data_driver.get_training_split(
        dataset_name=name,
        training_rate=training_rate,
        class_attribute=class_attribute,
        include_attributes=[],
        exclude_attributes=[],
        attributes_rate=0.0,
        random_seed=random_seed,
        output_csv=stream,
        include_header=include_header,
        class_only=True,
    )

    stream.seek(0)

    return flask.send_file(stream, mimetype='text/csv')


@app.route('/dataset/<string:name>/split/fusion/class', methods=['GET'])
def get_dataset_fusion_split_class(name):
    """
    Retrieves the class column from the dataset fusion split as a CSV file.
    GET: /dataset/<str:name>/split/fusion/class

    :param name: the name of the dataset
    :type name: str

    :param args['training_rate']: the percentage of the dataset to consider as training split
    :type args['training_rate']: float

    :param args['fusion_rate']: the percentage of the dataset to consider as fusion split
    :type args['fusion_rate']: float

    :param args['class_attribute']: the class attribute name
    :type args['class_attribute']: str

    :param args['random_seed']: the random seed
    :type args['random_seed']: int

    :param args['include_header']: if True, it includes the header in the output CSV
    :type args['include_header']: bool

    :return: the output file as an opened stream
    :rtype: file
    """
    data_driver = get_data_driver()

    training_rate = float(flask.request.args.get('training_rate'))
    fusion_rate = float(flask.request.args.get('fusion_rate'))
    class_attribute = flask.request.args.get('class_attribute')
    random_seed = int(flask.request.args.get('random_seed'))
    include_header = ast.literal_eval(flask.request.args.get('include_header'))

    stream = io.BytesIO()

    data_driver.get_fusion_split(
        dataset_name=name,
        training_rate=training_rate,
        fusion_rate=fusion_rate,
        class_attribute=class_attribute,
        include_attributes=[],
        exclude_attributes=[],
        attributes_rate=0.0,
        random_seed=random_seed,
        output_csv=stream,
        include_header=include_header,
        class_only=True,
    )

    stream.seek(0)

    return flask.send_file(stream, mimetype='text/csv')


@app.route('/dataset/<string:name>/split/test/class', methods=['GET'])
def get_dataset_test_split_class(name):
    """
    Retrieves the class column from the dataset test split as a CSV file.
    GET: /dataset/<str:name>/split/test/class

    :param name: the name of the dataset
    :type name: str

    :param args['training_rate']: the percentage of the dataset to consider as training split
    :type args['training_rate']: float

    :param args['fusion_rate']: the percentage of the dataset to consider as fusion split
    :type args['fusion_rate']: float

    :param args['class_attribute']: the class attribute name
    :type args['class_attribute']: str

    :param args['random_seed']: the random seed
    :type args['random_seed']: int

    :param args['include_header']: if True, it includes the header in the output CSV
    :type args['include_header']: bool

    :return: the output file as an opened stream
    :rtype: file
    """
    data_driver = get_data_driver()
    training_rate = float(flask.request.args.get('training_rate'))
    fusion_rate = float(flask.request.args.get('fusion_rate'))
    class_attribute = flask.request.args.get('class_attribute')
    random_seed = int(flask.request.args.get('random_seed'))
    include_header = ast.literal_eval(flask.request.args.get('include_header'))

    stream = io.BytesIO()

    data_driver.get_test_split(
        dataset_name=name,
        training_rate=training_rate,
        fusion_rate=fusion_rate,
        class_attribute=class_attribute,
        include_attributes=[],
        exclude_attributes=[],
        attributes_rate=0.0,
        random_seed=random_seed,
        output_csv=stream,
        include_header=include_header,
        class_only=True,
    )

    stream.seek(0)

    return flask.send_file(stream, mimetype='text/csv')


@app.route('/dataset/<string:name>/split/training/sample/class', methods=['GET'])
def get_dataset_training_sample_class(name):
    """
    Retrieves the class column from the dataset sample within the training split as a CSV file.
    GET: /dataset/<str:name>/split/training/sample/class

    :param name: the name of the dataset
    :type name: str

    :param args['training_rate']: the percentage of the dataset to consider as training split
    :type args['training_rate']: float

    :param args['sample_rate']: the percentage of instances, within the training split, to include
    :type args['sample_rate']: float

    :param args['sample_number']: the sample number starting from 0
    :type args['sample_number']: int

    :param args['class_attribute']: the class attribute name
    :type args['class_attribute']: str

    :param args['random_seed']: the random seed
    :type args['random_seed']: int

    :param args['include_header']: if True, it includes the header in the output CSV
    :type args['include_header']: bool

    :param args['class_only']: specifies if returning the class column only
    :type args['class_only']: bool

    :return: the output file as an opened stream
    :rtype: file
    """
    data_driver = get_data_driver()

    training_rate = float(flask.request.args.get('training_rate'))
    sample_rate = float(flask.request.args.get('sample_rate'))
    sample_number = int(flask.request.args.get('sample_number'))
    class_attribute = flask.request.args.get('class_attribute')
    random_seed = int(flask.request.args.get('random_seed'))
    include_header = ast.literal_eval(flask.request.args.get('include_header'))

    stream = io.BytesIO()

    data_driver.get_training_sample(
        dataset_name=name,
        training_rate=training_rate,
        sample_rate=sample_rate,
        sample_number=sample_number,
        class_attribute=class_attribute,
        include_attributes=[],
        exclude_attributes=[],
        attributes_rate=0.0,
        random_seed=random_seed,
        output_csv=stream,
        include_header=include_header,
        class_only=True,
    )

    stream.seek(0)

    return flask.send_file(stream, mimetype='text/csv')


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False)
