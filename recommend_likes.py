import mysql.connector
import time
import operator
import sys
import os.path

start_time = 0
def timer_start():
    global start_time
    start_time = time.time()
def timer_stop():
    global start_time
    #verbose output is suppressed
    #print "Action took:", time.time() - start_time, "seconds"

#return the user name and password for the MySQL server from the config.txt
def get_server_info():
    if os.path.isfile("config.txt"):
        config = open("config.txt")
        name = config.readline().rstrip()
        password = config.readline().rstrip()
        return name, password
    else:
        print "Error: 'config.txt' doesn't exist. Make sure config.txt exists, with the first line being the MySQL server's user name, and the second line being the password"
        exit(1)

class Page_Match:
    page = ""
    matches = {}
    likes = -1
    def __init__(self, name):
        self.page = name
        self.matches = {}
        self.likes = -1
    def add_page(self, new_page):
        if new_page != self.page:
            if new_page in self.matches:
                self.matches[new_page] += 1
            else:
                self.matches[new_page] = 1
    def normalize(self):
        #normalize based on the number of likes
        for m in self.matches.iteritems():
            self.matches[m[0]] = float(self.matches[m[0]])/self.likes

#initial setup
LIKES_THRESHOLD = 20
db_user, db_password = get_server_info()
cnx_test = mysql.connector.connect(user=db_user, password=db_password)
cursor = cnx_test.cursor()
cursor.execute("SHOW DATABASES LIKE 'likes'")
if not cursor.fetchone():
    print "Error: 'likes' database doesn't exist, try running setup.py first."
    sys.exit(1)
cursor.close()
cnx_test.close()

cnx = mysql.connector.connect(user=db_user, password=db_password, database='likes')
cursor1 = cnx.cursor(buffered=True)
cursor2 = cnx.cursor(buffered=True)

cursor1.execute("SHOW TABLES LIKE 'users'")
if not cursor1.fetchone():
    print "Error: 'users' table doesn't exist, try running setup.py first."
    sys.exit(1)
cursor1.execute("SHOW TABLES LIKE 'user_likes'")
if not cursor1.fetchone():
    print "Error: 'user_likes' table doesn't exist, try running setup.py first."
    sys.exit(1)
cursor1.execute("SHOW TABLES LIKE 'pages'")
if not cursor1.fetchone():
    print "Error: 'pages' table doesn't exist, try running setup.py first."
    sys.exit(1)
cursor1.execute("SHOW TABLES LIKE 'user_likes_culled'")
if not cursor1.fetchone():
    print "Error: 'users_likes_culled' table doesn't exist, try running make_pairs.py first."
    sys.exit(1)

#parse input
input_string = ' '.join(sys.argv[1:]).replace("'", "''").replace("\\", "\\\\")
if input_string == "":
    print "Error: No pages inputed. Proper usage is to have a"
    print "comma separated list of likes in the command line, such as:"
    print "recommend_likes.py Eminem,Kanye West,2pac"
    sys.exit(1)
#This is to help prevent against SQL injections. A more comprehensive approach is beyond
#the scope of this project
if input_string.find(";")>-1:
    print "Error: Invalid input character ';' (semi-colon)"
    exit(1)
input_pages = input_string.split(",")

timer_start()
inputs = []
user_likes = "user_likes_culled"
#Check the input page exists, get it's likes and check if we're dealing
#with any unpopular pages
for input in input_pages:
    cursor1.execute("SELECT likes FROM pages WHERE page='"+input+"';")
    likes = cursor1.fetchone()
    if likes == None:
        print "Warning:'"+input+"' was not found in pages database, and thus cannot be matched."
        print "Check the spelling, capitalization, or name of the official page you are looking for."
    #if the page was culled out, then we have to use the un-culled user_likes database
    elif likes[0] < LIKES_THRESHOLD:
        new_page_match = Page_Match(input)
        new_page_match.likes = likes[0]
        inputs.append(new_page_match)
        user_likes = "user_likes"
    else:
        new_page_match = Page_Match(input)
        new_page_match.likes = likes[0]
        inputs.append(new_page_match)

if len(inputs) == 0:
    print "Error: No valid inputs found."
    exit(1)
    
print "Getting likes recommendations. This may take a few moments..."
cursor1.execute("SELECT user_id FROM users;")
#For each input, get the other pages users liked with it, and the number of times
user = cursor1.fetchone()
on_user = 0
while user:
    page_set = []
    cursor2.execute("SELECT page FROM " + user_likes + " WHERE user_id = "+str(user[0])+";")
    to_process_pages = cursor2.fetchall()
    for page in to_process_pages:
        page = page[0].encode("utf8").replace("'", "''").replace("\\", "\\\\")
        page_set.append(page)
    #users can like pages multiple times, be we only want unique pages
    page_set = set(page_set)
    for input in inputs:
        if input.page in page_set:
            for page in page_set:
                input.add_page(page)
#     if on_user%1000 == 0:
#         print "At user number:",on_user
    on_user+=1
    user =cursor1.fetchone()
timer_stop()

#Calculate the scores of the different pages
final_matches = {}
for i in inputs:
    i.normalize()
    for match in i.matches.iteritems():
        if match[0] not in input_pages:
            if match[0] in final_matches:
                final_matches[match[0]] += float(match[1])/len(inputs)
            else:
                final_matches[match[0]] = float(match[1])/len(inputs)

#Output the top matching pages
print
print "Top 10 Page Matches:"
num_matches = 0
for m in sorted(final_matches.iteritems(), key=operator.itemgetter(1), reverse=True):
    print str(num_matches+1)+".", m[0].replace("''", "'").replace("\\\\", "\\")+",", "{0:.5f}".format(m[1])
    num_matches += 1
    if num_matches == 10:
        break


cursor1.close()
cnx.close()