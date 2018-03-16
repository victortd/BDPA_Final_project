# Question 5.3 1)

import sys
from string import punctuation

previous_espece = None
current_espece = None
current_count = 0

for row in sys.stdin:
    row = row.strip()
    current_espece, count = row.split('\t', 1)

    if previous_espece == None:
        previous_espece = current_espece

    if current_espece != previous_espece:
        print("%s\t%s" % (previous_espece, str(current_count)))
        current_count = 0
    
    # update for next iteration:
    previous_espece = current_espece
    current_count += 1

print("%s\t%s" % (previous_espece, str(current_count)))
