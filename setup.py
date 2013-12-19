import mysql.connector
import itertools
import re
import time
import os.path

start_time = 0
def timer_start():
    global start_time
    start_time = time.time()
def timer_stop():
    global start_time
    print "Action took:", time.time() - start_time, "seconds"

#return the user name and password for the MySQL server from config.txt
def get_server_info():
    if os.path.isfile("config.txt"):
        config = open("config.txt")
        name = config.readline().rstrip()
        password = config.readline().rstrip()
        return name, password
    else:
        print "Error: 'config.txt' doesn't exist. Make sure config.txt exists, with the first line being the MySQL server's user name, and the second line being the password"
        exit(1)

#parse the data from a line from the CSV
def parse(line):
    user_data = line.split('","')
    user_data[-1] = user_data[-1].rstrip('"\n')
    user_data[0] = user_data[0].lstrip('"')
    for i in range(0, len(user_data)):
        user_data[i] = user_data[i].replace("'", "''").replace("\\", "\\\\")
    return user_data



#initial set up
DB_NAME = 'likes'
db_user, db_password = get_server_info()
cnx = mysql.connector.connect(user=db_user, password=db_password)
cursor = cnx.cursor()
cursor.execute("DROP DATABASE IF EXISTS "+DB_NAME)
cursor.execute("CREATE DATABASE "+DB_NAME+" DEFAULT CHARACTER SET 'utf8'")
cursor.close()
cnx.close()

num_users = 0
cnx = mysql.connector.connect(user=db_user, password=db_password, database=DB_NAME)
cursor1 = cnx.cursor(buffered=True)
cursor2 = cnx.cursor(buffered=True)

MAX_PAGE_NAME_LENGTH = 100
cursor1.execute("DROP TABLE IF EXISTS users")
cursor1.execute(
    "CREATE TABLE `users` ("
    "  `user_id` int NOT NULL AUTO_INCREMENT,"
    "  `user_name` char(32) NOT NULL,"
    "  UNIQUE (`user_name`),"
    "  PRIMARY KEY (`user_id`)"
    ") ENGINE=InnoDB")
#ideally, this table would use an int page_id as well, but it must be constructed before the pages table
#and adding the page_id to user_likes was prohibitively time consuming.
cursor1.execute("DROP TABLE IF EXISTS user_likes")
cursor1.execute(
    "CREATE TABLE `user_likes` ("
    "  `user_id` int(10) NOT NULL,"
    "  `page` nvarchar("+str(MAX_PAGE_NAME_LENGTH)+") NOT NULL"
    ") ENGINE=InnoDB")
cursor1.execute("DROP TABLE IF EXISTS pages")
cursor1.execute(
    "CREATE TABLE `pages` ("
    "  `page` nvarchar("+str(MAX_PAGE_NAME_LENGTH)+") NOT NULL,"
    "  `likes` int(10) unsigned NOT NULL"
    ") ENGINE=InnoDB")


#Make users table, which is just a table with user_name and user_id
#Also make user_likes, which is a SQL version of likes.csv
timer_start()
print "Making users and user_likes tables..."
data_file = open("likes.csv")
line = data_file.readline()
next_line = data_file.readline()
execute_buffer = ""
new_page_likes = ""
while line != "":
    #parse the data
    #pages may sometimes contain newlines, so add to the line until we find the next user
    while next_line != "" and (len(next_line) < 34 or next_line[0] != '"' or next_line[33:36] != '","' or next_line[0:36].find('","') == -1 or not re.match("^[a-f0-9]*$", next_line[1:33])):
        line += next_line
        next_line = data_file.readline()
    user_data = parse(line)
    #this might seem like it's extra steps, but it's necessary because all lines aren't
    #necessarily unique users
    cursor1.execute("INSERT IGNORE INTO users (user_name) VALUES ('"+user_data[0]+"');")
    cursor1.execute("SELECT user_id FROM users WHERE user_name = '"+user_data[0]+"';")
    user_id = cursor1.fetchone()
    
    for i in range(1, len(user_data)):
        #we're ignoring pages with very long names
        #they are too long to be keys, and amount to almost none of the data
        #they would also be culled out and insignificant every step of the way
        if len(user_data[i]) <= MAX_PAGE_NAME_LENGTH:
            new_page_likes += "("+str(user_id[0])+",'"+user_data[i]+"'),"

    if num_users%5 == 0:
        if new_page_likes != "":
            execute_buffer ="INSERT INTO user_likes (user_id, page) VALUES "+new_page_likes.rstrip(",")+"; "
            new_page_likes = ""
            cursor1.execute(execute_buffer)
            execute_buffer = ""
    if num_users%500 == 0:
        print "At user number:",num_users
    num_users += 1
    line = next_line
    next_line = data_file.readline()
#make sure to execute anything left in the buffer
if new_page_likes != "":
    execute_buffer ="INSERT INTO user_likes (user_id, page) VALUES "+new_page_likes.rstrip(",")+"; "
    new_page_likes = ""
    cursor1.execute(execute_buffer)
    execute_buffer = ""
data_file.close()
timer_stop()

#build the pages table
print "Making pages table..." 
timer_start()
cursor1.execute("CREATE INDEX like_page_index ON user_likes (page);")
cursor1.execute("CREATE INDEX like_user_index ON user_likes (user_id);")
cursor1.execute("INSERT INTO pages (page, likes) SELECT page, count(*) FROM user_likes GROUP BY page;")
cursor1.execute("CREATE INDEX page_index ON pages (page);")
timer_stop()

# #uncomment this section if you want to output
# #the sorted pages table to a file
# timer_start()
# pages_file = open("pages.csv", mode="w")
# cursor1.execute("SELECT * FROM pages ORDER BY likes DESC;")
# for i in cursor1.fetchall():
#     pages_file.write(str(i[1]) + "`" + i[0].encode("utf8") +"\n")
# pages_file.close()
# timer_stop()


 
cursor1.close()
cursor2.close()
cnx.commit()
cnx.close()

print "Done."
