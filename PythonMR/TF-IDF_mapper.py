#!/usr/bin/env python

from __future__ import print_function
import sys
import re

def read_vocabulary (file_path):
	return set (word.strip() for word in open(file_path))

vocab = read_vocabulary ("stop_words_en.txt")


for line in sys.stdin:
	try:
		article_id, text = str(line.strip()).split('\t', 1)
	except ValueError as e:
		continue
	words = re.split("\W*\s+\W*", text)
	wc = 0
	for word in words:
		wc +=1
	for word in words:
		if word in vocab:
			key_str = str(wc)+";"+word.lower()+";"+article_id
			#print(str(wc),word.lower(),str(article_id),1, sep=";")
			print("%s\t%d" % (key_str,1))
