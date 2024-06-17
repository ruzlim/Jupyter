import os
import argparse
import logging
from glob import glob 
import pandas as pd
import numpy as np


# Test
def sayhi(name):
    text = f'Hello {name}!!!'
    return text


# Main
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--param_1', required=True)

    args = parser.parse_args()
    param_1 = args.param_1

    print(f'\n   -> param_1 : {param_1}\n')
    print(f'   -> {sayhi(param_1)}\n')


if __name__ == "__main__":
    main()
