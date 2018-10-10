<h3>About</h3>

A program in Spark or Hadoop that implements a simple “People You Might Know” social
network friendship recommendation algorithm. If two people have a lot of mutual friends, then the system should recommend they connect with each other.

<h3>Input</h3>

The input file contains the adjacency list and has multiple lines in the following format:
<User><TAB><Friends>
where <User> is a unique integer ID corresponding to a unique user and <Friends> is a comma-separated list of unique IDs corresponding to the friends of the user with the unique ID <User>.


<h3>Output</h3>
The output is in the following format:
<User><TAB><Recommendations>
where <User> is a unique ID corresponding to a user and Recommendations is a comma-separated list of unique IDs corresponding to the algorithm’s recommendation of people that <User> might know, ordered by decreasing number of mutual friends.