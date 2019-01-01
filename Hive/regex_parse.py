#!/usr/bin/env python

import sys
import re

regexp = '.*?(?=.*\\bId=\"(\\d+)\")(?=.*\\bPostTypeId=\"(\\d+)\")(?=.*\\bCreationDate=\"(\\S+)\")(?=.*\\bOwnerUserId=\"(\\d+)\")(?=.*\\bScore=\"(\\d+)\")?(((?=.*\\blt\;(.*)&gt\;)?.*'

for line in sys.stdin:
   print(re.match(regexp, line.strip()))
       #print (line.strip())
