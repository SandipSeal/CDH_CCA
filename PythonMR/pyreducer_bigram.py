#!/usr/bin/env python
"""A more advanced Mapper, using Python iterators and generators."""

from __future__ import print_function
import sys
import re
from collections import Counter

prev_word = None
word_cnt = 0
word_list = []

for line in sys.stdin:
	curr_word, cnt = line.split("\t", 1)
	cnt = int(cnt)
	word_list.append(curr_word)

count = Counter(word_list)
count = count.most_common()
for word,wc in count:
	print(word,wc,sep="\t")



