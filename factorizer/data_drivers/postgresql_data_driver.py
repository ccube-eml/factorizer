import random
import psycopg2
from factorizer import utils
from factorizer.data_drivers.data_driver import DataDriver


CREATE_TABLE_STATEMENT = 'CREATE TABLE {table_name_} ({columns_definitions_});'

COLUMN_DEFINITIONS_PATTERN = '"{name_}" {type_}'

COPY_FROM_CSV_WITH_HEADER_STATEMENT = 'COPY {table_name_} ' \
                                      'FROM STDIN ' \
                                      'WITH CSV HEADER ' \
                                      'DELIMITER AS \'{delimiter_}\''

COPY_FROM_CSV_WITHOUT_HEADER_STATEMENT = 'COPY {table_name_} ' \
                                         'FROM STDIN ' \
                                         'DELIMITER AS \'{delimiter_}\''

COPY_TO_CSV_WITH_HEADER_STATEMENT = 'SELECT SETSEED({random_seed_}); ' \
                                    'COPY ({statement_}) ' \
                                    'TO STDOUT ' \
                                    'WITH CSV HEADER ' \
                                    'DELIMITER AS \',\''

COPY_TO_CSV_WITHOUT_HEADER_STATEMENT = 'SELECT SETSEED({random_seed_}); ' \
                                       'COPY ({statement_}) ' \
                                       'TO STDOUT ' \
                                       'DELIMITER AS \',\''

DROP_TABLE_STATEMENT = 'DROP TABLE IF EXISTS {table_name_};'

SELECT_DISTINCT_STATEMENT = 'SELECT DISTINCT "{column_name_}" ' \
                            'FROM {table_name_};'

COUNT_STATEMENT = 'SELECT COUNT(*) ' \
                  'FROM {table_name_} ' \
                  'WHERE "{column_name_}" = \'{value_}\';'

GET_COLUMNS_NAMES_STATEMENT = 'SELECT * ' \
                              'FROM {table_name_} ' \
                              'LIMIT 0;'

SELECT_SAMPLE_STATEMENT = 'SELECT {attributes_} ' \
                          'FROM {table_name_} ' \
                          'WHERE "{class_attribute_}" = \'{class_attribute_value_}\' ' \
                          'ORDER BY RANDOM() ' \
                          'LIMIT {limit_} ' \
                          'OFFSET {offset_}'

ATTRIBUTE_TYPE_NAMES = {
    'integer': 'int',
    'real': 'numeric',
    'text': 'text',
}

DATA_CHUNK_SIZE = 4096


class PostgreSQLDataDriver(DataDriver):
    """
    Implements a data driver communicating with a PostgreSQL database.
    """

    def __init__(self, database, username, password, hostname, port):
        """
        Initializes the data driver.

        :param database: the name of the database
        :type database: str

        :param username: the username
        :type username: str

        :param password: the password
        :type password: str

        :param hostname: the hostname
        :type hostname: str

        :param port: the port
        :type port: str
        """
        super().__init__()

        self.__connection = psycopg2.connect(
            database=database,
            user=username,
            password=password,
            host=hostname,
            port=port
        )

        self.__cursor = self.__connection.cursor()

    def close(self):
        self.__cursor.close()
        self.__connection.close()

    def create_structure(
            self,
            name,
            attributes
    ):
        # Creates the structure.
        columns_definitions = self.__compose_columns_definitions(attributes)
        self.__cursor.execute(CREATE_TABLE_STATEMENT.format(table_name_=name, columns_definitions_=columns_definitions))
        self.__connection.commit()

    def destroy_structure(
            self,
            name,
    ):
        self.__cursor.execute(DROP_TABLE_STATEMENT.format(table_name_=name))
        self.__connection.commit()

    def fill_structure(
            self,
            name,
            delimiter,
            header,
            input_csv,
    ):
        if header:
            statement = COPY_FROM_CSV_WITH_HEADER_STATEMENT.format(table_name_=name, delimiter_=delimiter)
        else:
            statement = COPY_FROM_CSV_WITHOUT_HEADER_STATEMENT.format(table_name_=name, delimiter_=delimiter)

        self.__cursor.copy_expert(
            statement,
            file=input_csv,
        )
        self.__connection.commit()

    def get_training_split(
            self,
            dataset_name,
            training_rate,
            class_attribute,
            include_attributes,
            exclude_attributes,
            attributes_rate,
            random_seed,
            output_csv,
            include_header,
            class_only,
    ):
        return self._get_split(
            split_type=DataDriver.SplitType.training,
            dataset_name=dataset_name,
            training_rate=training_rate,
            fusion_rate=0,
            training_sample_rate=0,
            training_sample_number=0,
            class_attribute=class_attribute,
            include_attributes=include_attributes,
            exclude_attributes=exclude_attributes,
            attributes_rate=attributes_rate,
            random_seed=random_seed,
            output_csv=output_csv,
            include_header=include_header,
            class_only=False,
        )

    def get_fusion_split(
            self,
            dataset_name,
            training_rate,
            fusion_rate,
            class_attribute,
            include_attributes,
            exclude_attributes,
            attributes_rate,
            random_seed,
            output_csv,
            include_header,
            class_only,
    ):
        return self._get_split(
            split_type=DataDriver.SplitType.fusion,
            dataset_name=dataset_name,
            training_rate=training_rate,
            fusion_rate=fusion_rate,
            training_sample_rate=0,
            training_sample_number=0,
            class_attribute=class_attribute,
            include_attributes=include_attributes,
            exclude_attributes=exclude_attributes,
            attributes_rate=attributes_rate,
            random_seed=random_seed,
            output_csv=output_csv,
            include_header=include_header,
            class_only=class_only,
        )

    def get_test_split(
            self,
            dataset_name,
            training_rate,
            fusion_rate,
            class_attribute,
            include_attributes,
            exclude_attributes,
            attributes_rate,
            random_seed,
            output_csv,
            include_header,
            class_only,
    ):
        return self._get_split(
            split_type=DataDriver.SplitType.test,
            dataset_name=dataset_name,
            training_rate=training_rate,
            fusion_rate=fusion_rate,
            training_sample_rate=0,
            training_sample_number=0,
            class_attribute=class_attribute,
            include_attributes=include_attributes,
            exclude_attributes=exclude_attributes,
            attributes_rate=attributes_rate,
            random_seed=random_seed,
            output_csv=output_csv,
            include_header=include_header,
            class_only=class_only,
        )

    def get_training_sample(
            self,
            dataset_name,
            training_rate,
            sample_rate,
            sample_number,
            class_attribute,
            include_attributes,
            exclude_attributes,
            attributes_rate,
            random_seed,
            output_csv,
            include_header,
            class_only,
    ):
        return self._get_split(
            split_type=DataDriver.SplitType.training_sample,
            dataset_name=dataset_name,
            training_rate=training_rate,
            fusion_rate=0,
            training_sample_rate=sample_rate,
            training_sample_number=sample_number,
            class_attribute=class_attribute,
            include_attributes=include_attributes,
            exclude_attributes=exclude_attributes,
            attributes_rate=attributes_rate,
            random_seed=random_seed,
            output_csv=output_csv,
            include_header=include_header,
            class_only=class_only,
        )

    def _get_split(
            self,
            split_type,
            dataset_name,
            training_rate,
            fusion_rate,
            training_sample_rate,
            training_sample_number,
            class_attribute,
            include_attributes,
            exclude_attributes,
            attributes_rate,
            random_seed,
            output_csv,
            include_header,
            class_only,
    ):
        """
        Outputs the required split to a CSV output file.

        :param split_type: the split type
        :type split_type: DataDriver.SplitType

        :param dataset_name: the name of the dataset
        :type dataset_name: str

        :param training_rate: the percentage of the dataset to consider as training split
        :type training_rate: float

        :param fusion_rate: the percentage of the dataset to consider as fusion split
        :type fusion_rate: float

        :param class_attribute: the name of the class attribute
        :type class_attribute: str

        :param include_attributes: the list of the attributes to include, None otherwise
        :type include_attributes: list[str]

        :param exclude_attributes: the list of the attributes to exclude, None otherwise
        :type exclude_attributes: list[str]

        :param attributes_rate: the percentage of attributes to include
        :type attributes_rate: float

        :param random_seed: the random seed
        :type random_seed: int

        :param output_csv: the output file as an opened stream
        :type output_csv: file

        :param include_header: if True, it includes the header in the output CSV
        :type include_header: bool

        :param class_only: specifies if returning the class column only
        :type class_only: bool
        """
        # Prepares the random generator.
        random_generator = random.Random(random_seed)

        # Retrieves the attributes names except for the class attribute.
        attributes_names = self.__get_attributes_names(dataset_name, class_attribute)

        # Filters the attributes.
        attributes_names = self.__filter_attributes(attributes_names, include_attributes, exclude_attributes)

        # Generates the random list of attributes.
        attributes_sample_size = int(len(attributes_names) * attributes_rate)
        attributes_sample = utils.random_ordered_sample(
            random_generator,
            attributes_names,
            attributes_sample_size,
        )

        # Retrieves the possible class attribute values.
        class_attribute_values = self.__get_class_attribute_values(dataset_name, class_attribute)

        # For each class attribute value samples the data.
        is_first_partition = True
        for class_attribute_value in class_attribute_values:
            # Counts the number of instances for the current partition.
            current_partition_size = self.__get_partition_size(dataset_name, class_attribute, class_attribute_value)

            # Computes all the split sizes.
            current_partition_training_split_size = int(current_partition_size * training_rate)
            current_partition_fusion_split_size = int(current_partition_size * fusion_rate)
            current_partition_test_split_size =\
                current_partition_size - current_partition_training_split_size - current_partition_fusion_split_size

            # Discriminates according to the split type.
            current_partition_split_size = None
            current_partition_split_offset = None
            if split_type == DataDriver.SplitType.training:
                current_partition_split_size = current_partition_training_split_size
                current_partition_split_offset = 0
            elif split_type == DataDriver.SplitType.fusion:
                current_partition_split_size = current_partition_fusion_split_size
                current_partition_split_offset = current_partition_training_split_size
            elif split_type == DataDriver.SplitType.test:
                current_partition_split_size = current_partition_test_split_size
                current_partition_split_offset =\
                    current_partition_training_split_size + current_partition_fusion_split_size
            elif split_type == DataDriver.SplitType.training_sample:
                current_partition_split_size = int(current_partition_training_split_size * training_sample_rate)
                current_partition_split_offset = current_partition_split_size * training_sample_number

            # Copies the instances to the output CSV.
            self.__copy_instances_to_csv(
                table_name=dataset_name,
                attributes_sample=attributes_sample,
                class_attribute=class_attribute,
                class_attribute_value=class_attribute_value,
                split_size=current_partition_split_size,
                offset=current_partition_split_offset,
                is_first_partition=is_first_partition,
                random_seed=random_seed,
                output_csv=output_csv,
                include_header=include_header,
                class_only=class_only,
            )

            if is_first_partition:
                is_first_partition = False

    def __get_attributes_names(self, table_name, class_attribute):
        """
        Retrieves the list of attributes except for the class attribute.

        :param table_name: the name of the table
        :type table_name: str

        :param class_attribute: the class attribute name
        :type class_attribute: str

        :return: the list of attributes
        :rtype: list[str]
        """
        self.__cursor.execute(GET_COLUMNS_NAMES_STATEMENT.format(table_name_=table_name))
        attributes_names = [
            column.name
            for column in self.__cursor.description
            if column.name != class_attribute
        ]
        return attributes_names

    def __get_class_attribute_values(self, table_name, class_attribute):
        """
        Retrieves the class attribute values.

        :param table_name: the name of the table
        :type table_name: str

        :param class_attribute: the class attribute name
        :type class_attribute: str

        :return: the class attribute value
        :rtype: list[object]
        """
        self.__cursor.execute(
            SELECT_DISTINCT_STATEMENT.format(
                column_name_=class_attribute,
                table_name_=table_name,
            )
        )
        class_attribute_values = [value[0] for value in self.__cursor.fetchall()]
        return class_attribute_values

    def __get_partition_size(self, table_name, class_attribute, class_attribute_value):
        """
        Retrieves the number of instances in the partition.

        :param table_name: the name of the table
        :type table_name: str

        :param class_attribute: the class attribute name
        :type class_attribute: str

        :param class_attribute_value: the value of the class attribute
        :type class_attribute_value: object

        :return: the partition size
        :rtype: int
        """
        self.__cursor.execute(
            COUNT_STATEMENT.format(
                table_name_=table_name,
                column_name_=class_attribute,
                value_=class_attribute_value,
            )
        )
        current_partition_size = self.__cursor.fetchone()[0]
        return current_partition_size

    def __copy_instances_to_csv(
            self,
            table_name,
            attributes_sample,
            class_attribute,
            class_attribute_value,
            split_size,
            offset,
            is_first_partition,
            random_seed,
            output_csv,
            include_header,
            class_only,
    ):
        """
        Copies the instances to the output CSV file.

        :param table_name: the name of the table
        :type table_name: str

        :param attributes_sample: the list of attributes to select
        :type attributes_sample: list[str]

        :param class_attribute: the name of the class attribute
        :type class_attribute: str

        :param class_attribute_value: the value of the class attribute
        :type class_attribute_value: object

        :param split_size: the number of the instances to obtain
        :type split_size: int

        :param offset: the offset from the instance number 0
        :type offset: int

        :param is_first_partition: specifies if is the first partition that is treated
        :type is_first_partition: bool

        :param random_seed: the random seed
        :type random_seed: int

        :param output_csv: the output file as an opened stream
        :type output_csv: file

        :param include_header: if True, it includes the header in the output CSV
        :type include_header: bool

        :param class_only: specifies if returning the class column only
        :type class_only: bool
        """
        if class_only:
            formatted_attributes_sample = ['"' + class_attribute + '"']
        else:
            formatted_attributes_sample = ['"' + x + '"' for x in attributes_sample]
            formatted_class_attribute_name = '"' + class_attribute + '"'
            formatted_attributes_sample.append(formatted_class_attribute_name)

        statement = SELECT_SAMPLE_STATEMENT.format(
            attributes_=', '.join(formatted_attributes_sample),
            table_name_=table_name,
            class_attribute_=class_attribute,
            class_attribute_value_=class_attribute_value,
            limit_=split_size,
            offset_=offset,
        )
        if is_first_partition and include_header:
            self.__cursor.copy_expert(
                COPY_TO_CSV_WITH_HEADER_STATEMENT.format(
                    random_seed_=random_seed,
                    statement_=statement,
                ),
                file=output_csv,
            )
        else:
            self.__cursor.copy_expert(
                COPY_TO_CSV_WITHOUT_HEADER_STATEMENT.format(
                    random_seed_=random_seed,
                    statement_=statement,
                ),
                file=output_csv,
            )

    @staticmethod
    def __compose_columns_definitions(attributes):
        """
        Composes a string containing the attibutes and their types.

        :param attributes: the list of the attribute names
        :type attributes: list[str]

        :return: the string of attributes with types
        :rtype: str
        """
        columns_definitions = []
        for attribute in attributes:
            columns_definitions.append(
                COLUMN_DEFINITIONS_PATTERN.format(
                    name_=attribute['name'],
                    type_=ATTRIBUTE_TYPE_NAMES[attribute['type']]
                )
            )
        return ', '.join(columns_definitions)

    @staticmethod
    def __filter_attributes(attributes, include, exclude):
        """
        Retrieves the list of the attribute names based on the lists of include and exclude attributes.

        :param attributes: the original list of attributes
        :type attributes: list[str]

        :param include: the attributes to include
        :type include: list[str]

        :param exclude: the attributes to exclude
        :type exclude: list[str]

        :return: the list of filtered attributes
        :rtype list[str]
        """
        if include:
            return include

        if not exclude:
            return attributes

        filtered_attributes = []
        for attribute in attributes:
            if attribute not in exclude:
                filtered_attributes.append(attribute)
        return filtered_attributes
