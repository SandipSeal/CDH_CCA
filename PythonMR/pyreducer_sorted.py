#!/usr/bin/env python
"""A more advanced Mapper, using Python iterators and generators."""

from __future__ import print_function
import sys
import re

word_list = {}
key_pos = 0

for line in sys.stdin:
	curr_word, cnt = line.split("\t", 1)
	cnt = int(cnt)
	word_list.update({curr_word:cnt})

word_list_sort = sorted(word_list, key = word_list.get, reverse=True)
if sys.argv[1]:
	key_pos = int(sys.argv[1])-1
key = word_list_sort[key_pos]
print(key,word_list[key],sep="\t")
