## Usage
- prerequisities: sbt, java 11

sbt "run [input file] [output file] [algorithm = 1 | 2 | 3]"

## Configuration
Implemented in a separate case class to decouple this from main app arguments,
encapsulate validation and allow to be used as a domain object in other parts of the app.


## File reading
No 3rd party libraries are needed for this simple use case, custom `TupleFileReader`
has been implemented based on RegEx-es. Some level of robustness has been 
achieved by ignoring whitespaces. 


RDD is enough for this task, no need for DataFrame


# Algorithms

Algorithm 1:
XOR bitwise cancels out the same numbers, so we can use it to find the number with odd occurences (as it is there 2k + 1 times, 2k cancels out, the last occurence stays there and hence is a result of XORing the whole list of values)

Algorithm 2:
We can cancel out the number with even number of occurences within a single partition, this will reduce the space complexity (in the approximate mean case, not in the worst case).
Usage of map for deciding whether a number is there odd/even times.

Algorithm 3:
Groups numbers by keys, then for each of them finds the oddly occuring number by 
grouping by identity and counting the number of occurences. 