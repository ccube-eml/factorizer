from abc import ABCMeta, abstractmethod
import enum


class DataDriver(object):
    """
    Defines a data driver.
    """

    __metaclass__ = ABCMeta

    class SplitType(enum.Enum):
        training = 'training'
        fusion = 'fusion'
        test = 'test'
        training_sample = 'training_sample'

    def __init__(self):
        pass

    def close(self):
        """
        Closes the driver.
        """
        pass

    @abstractmethod
    def create_dataset(
            self,
            name,
            attributes,
    ):
        """
        Creates the dataset skeleton.

        :param name: the name of the dataset
        :type name: str

        :param attributes: the list of attributes in the form {'name': str, 'type': 'integer' | 'real' | 'text'}
        :type attributes: list[dict[str, str]]
        """
        pass

    @abstractmethod
    def destroy_dataset(
            self,
            name,
    ):
        """
        Destroys the dataset.

        :param name: the name of the dataset
        :type name: str
        """
        pass

    @abstractmethod
    def fill_structure(
            self,
            name,
            delimiter,
            header,
            input_csv,
    ):
        """
        Fills the dataset with the provided CSV file.

        :param name: the name of the dataset
        :type name: str

        :param delimiter: the delimiter used in the CSV file (ex. ',')
        :type delimiter: str

        :param header: specifies if the CSV file has a header
        :type header: bool

        :param input_csv: the input file as an opened stream
        :type input_csv: file
        """
        pass

    @abstractmethod
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
        """
        Retrieves a random dataset split for the training.

        :param dataset_name: the name of the dataset
        :type dataset_name: str

        :param training_rate: the percentage of the dataset to consider as training split
        :type training_rate: float

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
        pass

    @abstractmethod
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
        """
        Retrieves a random dataset split for the fusion.

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
        pass

    @abstractmethod
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
        """
        Retrieves a random dataset split for the test.

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
        pass

    @abstractmethod
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
        """
        Retrieves a random dataset sample in the CSV format.

        :param dataset_name: the name of the dataset
        :type dataset_name: str

        :param training_rate: the percentage of the dataset to consider as training split
        :type training_rate: float

        :param sample_rate: the percentage of instances, within the training split, to include
        :type sample_rate: float

        :param sample_number: the sample number starting from 0
        :type sample_number: int

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
        pass
