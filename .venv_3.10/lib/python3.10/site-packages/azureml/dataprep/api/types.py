# Copyright (c) Microsoft Corporation. All rights reserved.
from typing import TypeVar, Tuple, List, Dict

SplitExample = TypeVar('SplitExample', Tuple[str, List[str]], Tuple[Dict[str, str], List[str]])
Delimiters = TypeVar('Delimiters', str, List[str])
