#!/usr/bin/env python
"""A more advanced Mapper, using Python iterators and generators."""

from __future__ import print_function
import sys
import re

for line in sys.stdin:
	article_id, text = str(line.strip()).split("\t",1)
	text = re.sub('^\W+|W+$', '',text)
	words = re.split("\W+", text)
	for word in words:
		if word:
			print(word.lower()+"\t1")




