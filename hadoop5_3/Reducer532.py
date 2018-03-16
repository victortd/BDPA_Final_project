# Question 5.3 2)

import sys
from string import punctuation

# dictionary with 'espece' and their corresponding heights.
espece_and_height = {}

for row in sys.stdin:

    espece, tree_height = row.strip().split('\t')

    if espece in espece_and_height:
       espece_and_height[espece].append(tree_height)
    else:
       espece_and_height[espece] = []
       espece_and_height[espece].append(tree_height)

# get the maximum height for each 'espece':
for espece in espece_and_height.keys():
    highest_tree = max(espece_and_height[espece])
    print('%s\t%s'% (espece, highest_tree))
