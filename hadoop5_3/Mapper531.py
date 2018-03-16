# Question 5.3 1)

# We considered 'espece' as the type of tree.

import sys
from string import punctuation

count_tree = 0
for row in sys.stdin: 
    
    row = row.strip()
    data = row.split(';')
    
    ESPECE = data[3]

    if count_tree != 0:
        print("%s\t%s" % (ESPECE, '1'))
    
    count_tree += 1

