
#!/usr/bin/env python

"""A more advanced Mapper, using Python iterators and generators."""

import sys
import re
from collections import Counter


def read_words(file):
    for line in file:
        # split the line into words
        yield from re.findall(r'[a-z](?:[a-z\'‘’]*[a-z])?', line.lower())


def main():
    for word in read_words(sys.stdin):
        print(f'{word}\t1')


if __name__ == "__main__":
    main()
