#!/usr/bin/env python
"""A more advanced Mapper, using Python iterators and generators."""

from __future__ import print_function
import sys
import re

prev_word = None
word_cnt = 0

for line in sys.stdin:
	curr_key, cnt = line.split("\t", 1)
	cnt = int(cnt)
	if curr_word == prev_word:
		word_cnt += cnt
	else:
		if prev_word:			
			print(prev_word,str(word_cnt), sep="\t")
		prev_word = curr_word
		word_cnt = cnt
if prev_word:	
	print(prev_word,str(word_cnt), sep="\t")
