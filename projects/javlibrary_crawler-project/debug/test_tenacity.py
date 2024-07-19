# -*- coding: utf-8 -*-

"""
Test the exponential backoff feature of tenacity. We need this for crawler.
"""

import random
from datetime import datetime
from tenacity import retry, wait_exponential


st = datetime.now()

error_prob = 100


def do_it():
    elapse = "%.2f" % (datetime.now() - st).total_seconds()
    print(f"--- Start at {elapse}---")
    if random.randint(1, 100) <= error_prob:
        print("  Failed!")
        raise Exception
    else:
        print("  Succeeded!")


@retry(wait=wait_exponential(multiplier=1, min=4, max=60))
def do_it_with_retry():
    do_it()


if __name__ == "__main__":
    do_it_with_retry()
