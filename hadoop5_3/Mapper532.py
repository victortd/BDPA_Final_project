# Question 5.3 2)

# We considered 'espece' as type of the tree.

import sys
from string import punctuation

for row in sys.stdin:

    if row.split(";")[6].strip():
    	data = row.strip().split(';')
     	espece = data[3]
     	tree_height = data[6]
     	print('%s\t%s' % (espece, tree_height))



    
