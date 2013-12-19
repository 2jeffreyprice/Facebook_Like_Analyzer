import mysql.connector
import time
import operator
from operator import attrgetter
import heapq
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
            
class User_Match:
    user_id = -1
    pages = []
    score = -1
    def __init__(self):
        self.user_id = -1
        self.pages = []
        self.score = -1


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
input_string = ' '.join(sys.argv[2:]).replace("'", "''").replace("\\", "\\\\")
if input_string == "" or (sys.argv[1] != "likes" and sys.argv[1] != "users"):
    print "Error: Invalid input. Proper usage is to designate 'likes' followed"
    print "by a comma separated list of likes, such as:"
    print "recommend_users.py likes Eminem,Kanye West,2pac"
    print
    print "or to designate 'users' followed by a comma separated list of users:"
    print "recommend_users.py users user_name,user_name,user_name"
    sys.exit(1)
#This is to help prevent against SQL injections. A more comprehensive approach is beyond
#the scope of this project
if input_string.find(";")>-1:
    print "Error: Invalid input character ';' (semi-colon)"
    exit(1)
input_values = input_string.split(",")
input_is_users = True
if sys.argv[1] == "likes":
    input_is_users = False

timer_start()
input_pages = []
input_user_ids = []
#if we got users as an input, we need to get the pages they liked
if input_is_users:
    for user in input_values:
        cursor1.execute("SELECT user_id FROM users WHERE user_name='"+user+"';")
        user_id = cursor1.fetchone()
        if user_id:
            input_user_ids.append(user_id)
            cursor1.execute("SELECT page FROM user_likes WHERE user_id="+str(user_id[0])+";")
            pages = cursor1.fetchall()
            for page in pages:
                input_pages.append(page[0].encode("utf8"))
        else:
            print "Error: Invalid User Name'"+user+"'"
            exit(0)
else:
    input_pages = input_values

#Check the input page exists, get it's likes and check if we're dealing
#with any unpopular pages
inputs = []
user_likes = "user_likes_culled"
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
    
print "Getting user recommendations. This may take a few moments..."    
#print "First Pass"
#For each input, get the other pages users liked with it, and the number of times
cursor1.execute("SELECT user_id FROM users;")
user = cursor1.fetchone()
on_user = 0
while user:
    page_set = []
    cursor2.execute("SELECT page FROM " + user_likes + " WHERE user_id = "+str(user[0])+";")
    to_process_pages = cursor2.fetchall()
    for page in to_process_pages:
        page = page[0].encode("utf8").replace("'", "''").replace("\\", "\\\\")
        page_set.append(page)
    #users can like a page multiple times (or like pages with the same name), but we only want unique pages
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

for i in inputs:
    i.normalize()


timer_start()
#print "Second Pass"
#Get the user score for each user
cursor1.execute("SELECT user_id FROM users;")
user = cursor1.fetchone()
top_users = []
on_user = 0
while user:
    #don't want to return a user from the query
    if user[0] not in input_user_ids:
        new_user = User_Match()
        new_user.user_id = user[0]
        page_set = []
        cursor2.execute("SELECT page FROM " + user_likes + " WHERE user_id = "+str(user[0])+";")
        to_process_pages = cursor2.fetchall()
        for page in to_process_pages:
            page = page[0].encode("utf8").replace("'", "''").replace("\\", "\\\\")
            page_set.append(page)
        page_set = set(page_set)
        new_user.score = 0
        #check for exact matches
        inputs_left = inputs[:]
        for input in inputs:     
            if input.page in page_set:
                new_user.score+=float(1.0)/len(inputs)
                page_set.remove(input.page)
                inputs_left.remove(input)
                new_user.pages.append(input.page)
        #if there are close page matches needed, find the best fit pages from the remaining pages in page set
        #based on the inputs we couldn't match exactly
        matches_needed = len(inputs_left)
        if matches_needed > 0:
            page_scores = {}
            for page in page_set:
                page_score = 0
                for input in inputs_left:
                    if page in input.matches:
                        page_score+=float(input.matches[page])/matches_needed
                page_score /= len(inputs)
                page_scores[page] = page_score
            matching_pages = dict(sorted(page_scores.iteritems(), key=operator.itemgetter(1), reverse=True)[:matches_needed])
            new_user.score+= sum(matching_pages.values())
            for key in matching_pages.keys():
                new_user.pages.append(key)
        if not top_users or (top_users and len(top_users) < 10):
            top_users.append(new_user)
        else:
            min_top_user = min(top_users,key=attrgetter('score'))
            if new_user.score > min_top_user.score:
                top_users.remove(min_top_user)
                top_users.append(new_user)
#     if on_user%1000 == 0:
#         print "At user number:",on_user
    on_user+=1
    user =cursor1.fetchone()
timer_stop()

#Print the results
print
print "Top 10 User Recommendations"
top_users.sort(reverse=True, key=attrgetter('score'))
user_rank = 1
for user in top_users:
    cursor1.execute("SELECT user_name FROM users WHERE user_id="+str(user.user_id)+";")
    user_name = cursor1.fetchone()
    #It's possible to output all the pages a user liked instead, but users often like MANY
    #pages, this makes for a cluttered, unclear display
    #Instead, we output just the pages that matched the input
    print str(user_rank)+". User Name:", user_name[0]+", User Score:", "{0:.5f}".format(user.score)
    print "With the matching pages:"
    output_pages = ""
    for page in user.pages:
        output_pages+= str(page).replace("''", "'").replace("\\\\", "\\")+", "
    print output_pages.rstrip(", ")
    print
    user_rank+=1



cursor1.close()
cursor2.close()
cnx.close() 