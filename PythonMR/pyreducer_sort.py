#!/usr/bin/env python
"""A more advanced Mapper, using Python iterators and generators."""

from __future__ import print_function
import sys
import re

prev_word = None
word_cnt = 0
word_list = {}
key_pos = 0

for line in sys.stdin:
	curr_word, cnt = line.split("\t", 1)
	cnt = int(cnt)
	if curr_word == prev_word:
		word_cnt += cnt
	else:
		if prev_word:	
			word_list.update({prev_word:word_cnt})
		prev_word = curr_word
		word_cnt = cnt
if prev_word:
	word_list.update({prev_word:word_cnt})

word_list_sort = sorted(word_list, key = word_list.get, reverse=True)
key_pos = int(sys.argv[1])-1
key = word_list_sort[key_pos]
print(key,word_list[key],sep="\t")
