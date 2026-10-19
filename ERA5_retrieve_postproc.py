#!/usr/bin/env python3
"""Backward compatible wrapper: equivalent to the `cds-retriever` command"""

import sys

from cds_retriever.cli import main

if __name__ == "__main__":
    sys.exit(main())
