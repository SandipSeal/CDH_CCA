#!/usr/bin/env python
"""A more advanced Mapper, using Python iterators and generators."""

from __future__ import print_function
import sys
import re
import math

prev_word = None
word_cnt = 0

for line in sys.stdin:
	curr_word, article_id,tf = line.split(";")
	tf = float(tf)
	if curr_word == prev_word:
		word_cnt += 1
	else:
		if prev_word:
			idf_word = 1/math.log(1+word_cnt)		
			print("%s;%f" % (prev_word,idf_word))
		prev_word = curr_word
		word_cnt = 1
if prev_word:
	idf_word = 1/math.log(1+word_cnt)
	print("%s;%f" % (prev_word,idf_word))
