import mysql.connector
import itertools
import re
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
    print "Action took:", time.time() - start_time, "seconds"

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


#initial set up
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



MAX_PAGE_NAME_LENGTH = 100
cursor1.execute("DROP TABLE IF EXISTS user_likes_culled")
cursor1.execute(
    "CREATE TABLE `user_likes_culled` ("
    "  `user_id` int(10) NOT NULL,"
    "  `page` nvarchar("+str(MAX_PAGE_NAME_LENGTH)+") NOT NULL"
    ") ENGINE=InnoDB")


#make user_likes_culled, a table based on user_likes, but without the pages with fewer than LIKES_THRESHOLD
print "Making user_likes_culled table..."
timer_start()
#cursor1.execute("INSERT INTO user_likes_culled (user_id, page) SELECT user_likes.user_id, pages.page FROM user_likes INNER JOIN pages ON user_likes.page = pages.page WHERE pages.likes >="+str(LIKES_THRESHOLD)+";")
#This is an instance where one line of SQL code could have accomplished the same thing, but this method was faster on my computer
cursor1.execute("SELECT user_id FROM users;")
user = cursor1.fetchone()
on_user = 0
while user:
    cursor2.execute("INSERT INTO user_likes_culled SELECT "+str(user[0])+", pages.page FROM user_likes INNER JOIN pages ON user_likes.page = pages.page WHERE pages.likes>="+str(LIKES_THRESHOLD)+" AND user_likes.user_id="+str(user[0])+";")
    if on_user%500 == 0:
        print "At user number:",on_user
    on_user+=1
    user =cursor1.fetchone()
cursor1.execute("CREATE INDEX like_page_culled_index ON user_likes_culled (page);")
cursor1.execute("CREATE INDEX like_user_culled_index ON user_likes_culled (user_id);")
timer_stop()
cnx.commit()


timer_start()
page_pairs = {}
print "Getting page pairs..."
cursor1.execute("SELECT user_id FROM users;")
user = cursor1.fetchone()
on_user = 0
while user:
    page_set = []
    cursor2.execute("SELECT page FROM user_likes_culled WHERE user_id = "+str(user[0])+";")
    to_process_pages = cursor2.fetchall()
    for page in to_process_pages:
        page = page[0].encode("utf8")
        page_set.append(page)
    if len(page_set) > 1:
        #Since users are allowed to have likes of pages with the same name,
        #we explicitly only consider unique pairs per user, and pairs that are not a page paired with itself
        pairs = set(itertools.combinations(set(page_set),2))
        for pair in pairs:
            page1 = ""
            page2 = ""
            #make sure it's sorted, so that each pair is unique
            if pair[0]<pair[1]:
                page1 = pair[0]
                page2 = pair[1]
            else:
                page1 = pair[1]
                page2 = pair[0]
            #this string of characters ("@&$%#") is unique, although nothing prevents a
            #page from having this in it's name, which could cause problems
            #work arounds exist for that, but aren't necessary in this case
            pair = page1+" @&$%# "+page2
            if pair in page_pairs:
                page_pairs[pair] += 1
            else:
                page_pairs[pair] = 1
    if on_user%500 == 0:
        print "At user number:",on_user
    on_user+=1
    user =cursor1.fetchone()
timer_stop()

#output sorted pages table to file for analysis
timer_start()
print "Writing top pairs to file..."
pairs_file = open("page_pairs.txt", mode="w")
for pair in sorted(page_pairs.iteritems(), key=operator.itemgetter(1), reverse=True):
    if pair[1] >= LIKES_THRESHOLD:
        pairs_file.write(str(pair[1])+"`"+pair[0].replace(" @&$%#", ",")+"\n")
    else:
        break
pairs_file.close()
timer_stop()


cursor1.close()
cnx.commit()
cnx.close()

print "Done."