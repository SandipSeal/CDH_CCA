#!/usr/bin/env python
"""A more advanced Mapper, using Python iterators and generators."""

from __future__ import print_function
import sys
import re

reload(sys)
sys.setdefaultencoding('utf-8') # required to convert to unicode

for line in sys.stdin:
	for line in sys.stdin:
		try:
			article_id, text = str(line.strip()).split('\t', 1)
		except ValueError as e:
			continue
		words = re.split("\W*\s+\W*", text, flags = re.UNICODE)
		for word in words:
			if word:
				print(word.lower(), 1, sep="\t")
