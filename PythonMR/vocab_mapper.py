import sys
import re
import importlib

def read_vocabulary (file_path):
	return set (word.strip() for word in open(file_path))

vocab = read_vocabulary ("stop_words_en.txt")

importlib.reload(sys)
"""sys.setdefaultencoding('utf-8') # required to convert to unicode"""

index = 0

for line in sys.stdin:
	try:
		article_id, text = str(line.strip()).split('\t', 1)
	except ValueError as e:
		continue
	words = re.split("\W*\s+\W*", text, flags=re.UNICODE)
	for word in words:
		index += 1
		"""print("reporter:counter:Word Counter,Total Word,1",file=sys.stderr)"""
		if word in vocab:
			"""print("reporter:counter:Word Counter,Stop Word,1",file=sys.stderr)"""
			print(word.lower(), 1, sep="\t")
print('Total_Word_Count',index,sep="\t")
