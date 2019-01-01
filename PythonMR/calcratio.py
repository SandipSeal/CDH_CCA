#!/usr/bin/env python
"""A more advanced Mapper, using Python iterators and generators."""

from __future__ import print_function
import sys
import re

total_wc = 0
stop_wc = 0
word_list = {}

for line in sys.stdin:
	curr_word, cnt = line.split("\t", 1)
	cnt = int(cnt)
	if curr_word == 'Total_Word_Count':
		total_wc= cnt
	else:
		stop_wc += cnt

print(round(stop_wc/total_wc,8), sep="\t")
