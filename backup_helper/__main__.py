import sys

from backup_helper import cli


def main():
    cli.main(sys.argv[1:])


if __name__ == '__main__':
    main()
