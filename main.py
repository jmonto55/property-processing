import sys
from process_data import DataSourceIdentifier


def validate_args():
    if len(sys.argv) != 2:
        sys.exit(1)
    return sys.argv[1]


def main():
    json_file_path = validate_args()

    identifier = DataSourceIdentifier(json_file_path)
    result = identifier.determine_data_source()

    print(result)


if __name__ == "__main__":
    main()
