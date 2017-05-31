import json
import sys
import os
import csv

# print("""Before use, make sure you are in the directory one above the folder containing the
# all the data you want to process so that you can run -r.""")

# all json objects are delimited by \n so need to read-in line by line and then do json.load

print("Usage: once you've downloaded a json file to the data directory, type:")
print('python3 json_parser.py "data/2008onwards000000000049.json"')

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
        if user != "[deleted]":
            comment = str(data["body"])
            comment = comment.replace(",", "")
            comment = comment.replace("\n", "")
            comment = comment.replace("\r", "")
            # once we've stripped the comment correctly then
            # comment hasn't been correctly stripped yet

            outstring = [user + "," + comment]    
            print('Processed {} jsons'.format(counter))
            out.writerow(outstring)
            counter += 1


    print("Done.")



