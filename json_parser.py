import json
import sys
import os
import csv

# print("""Before use, make sure you are in the directory one above the folder containing the
# all the data you want to process so that you can run -r.""")

# all json objects are delimited by \n so need to read-in line by line and then do json.load

print("Usage: once you've downloaded a json file to the data directory, type:")
print('python3 json_parser.py "data/sample_data.json"')

pathname = os.getcwd() 
base_path = os.path.abspath(pathname) + "/"
full_path = base_path + sys.argv[1]
print("full path: ", full_path)

if not os.path.exists(full_path):
    print("Error with pathname. Please try again.")
    sys.exit(0)

outfile_name = "data_formatted.csv"
out = csv.writer(open(outfile_name,"w"), delimiter='\n', quoting=csv.QUOTE_ALL)
counter = 0

with open(full_path) as data_file:  
    out_line = "" 
    for line in data_file:
        data = json.loads(line)
        user = data["author"]
        if user is not "[deleted]":
            comment = data["body"]
            comment.strip(",")
            comment.strip()
            # once we've stripped the comment correctly then
            # comment hasn't been correctly stripped yet

            outstring = [user + "," + comment]    
            print('Processed {} jsons'.format(counter))
            out.writerow(outstring)
            counter += 1


    print("Done.")

'''
{"body":"hmm, interesting.","score_hidden":false,"archived":true,"name":"t1_c02sb9r","author":"[deleted]","downs":"0","created_utc":"1199161440","subreddit_id":"t5_2cneq","link_id":"t3_648up","parent_id":"t1_c02sb97","score":"0","retrieved_on":"1425820178","controversiality":"0","gilded":"0","id":"c02sb9r","subreddit":"politics","ups":"0"}

'''
