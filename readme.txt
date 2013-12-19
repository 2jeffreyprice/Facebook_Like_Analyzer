This Facebook likes analyzer parses an anonymized CSV Face book like data, formatted as “UserID”,“Like1”, “Like2”, etc, and builds several tables from the data. These tables are used for recommend_likes.py and recommend_users.py to recommmend likes and users based on the input.

The CSV is messy, as it contains pages with newlines, non-english characters, and other unicode characters. This project was intended to run on a much larger dataset (~177,000 users), but for demonstration purposes, a smaller dataset is included (~7500 users).


To run this code, you will need:
    -Python 2.7
    -MySQL Connector/Python for Python 2.7 from dev.mysql.com/downloads/connector/python
    -A running MySQL server
    -To modify config.txt to have the MySQL server credentials (username as the first line, password as the second line)
    -The “likes.csv” to be in the same folder

Run Order:
    1. Run “setup.py” first. If you want the pages information, you can uncomment the section towards the end of the script that outputs the pages data to a file.
    2. Run “make_pairs.py” second. This will output the a sorted list of pairs, up to the threshold, to a file.
    3. Then you are then free to run “recommend_likes.py” and “recommend_users.py”




Instructions for recommend_likes.py:
To use this script, just have the comma separated list of likes you wish to input as a parameter:
recommend_likes.py Kanye West,Jay-Z,Dr. Dre

The output is the top ten matching pages and the strength of the match. A “1.0” is the best possible match, but in my testing, around “0.5” is more typical of a good match.

NOTE: Make sure that the spelling and capitalization are correct, and that you are using the name of the official page. Sometimes the official page name is unintuitive.



Instructions for recommend_users.py:
The use of this script is similar to “recommend_likes.py”, except now you must designate whether the comma separated input is “likes” or “users” with the first parameter. For example:
recommend_users.py likes Kanye West,Jay-Z,Dr. Dre
or
recommend_users.py users user_name,user_name,user_name

If the input is users, the script will get the list of likes from those users to be used as the input set of likes.

The output is the top ten user matches, each listing the user name, the matching score, and the user's pages that match the input. A perfect score is “1.0”, meaning that the user liked all of the input pages.


NOTE: Again, make sure that the spelling and capitalization are correct, and that you are using the name of the official page.