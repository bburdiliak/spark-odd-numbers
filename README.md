RDD is enough for this task, no need for DataFrame


Algorithm 1:
XOR bitwise cancels out the same numbers, so we can use it to find the number with odd occurences (as it is there 2k + 1 times, 2k cancels out, the last occurence stays there and hence is a result of XORing the whole list of values)


Algorithm 2:
We can cancel out the number with even number of occurences within a single partition, this will reduce the space complexity (in the approximate mean case, not in the worst case).
Usage of map for deciding whether a number is there odd/even times.

Algorithm 3:
Similarily to Algorithm 2, just using setsinstead of maps.