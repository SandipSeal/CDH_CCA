#!/usr/bin/env python
"""A more advanced Mapper, using Python iterators and generators."""

from __future__ import print_function
import sys
import re

for line in sys.stdin:
	content = re.sub('^\W+|W+$', '',line)
	words = re.split("\W+",content)
	for idx in range(len(words)-1):
		print(words[idx],words[idx+1],'1')
