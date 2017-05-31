import json
import sys
import os, os.path
import csv

# count for this json = 835665 lines

folder = sys.argv[1]

outfile_name = "data_formatted.csv"
out = csv.writer(open(outfile_name,"w"), delimiter='\n', quoting=csv.QUOTE_ALL)

dir_contents = os.listdir(folder)
num_files = len(dir_contents)
file_counter = 0
counter = 0

pathname = os.getcwd() 
base_path = os.path.abspath(pathname) + "/" + folder

for f in dir_contents:
    full_path = base_path + "/" + f
    print(full_path)
    with open(full_path) as data_file:
        print("Now processing file {} of {}".format(file_counter, num_files))
        out_line = "" 
        for line in data_file:
            data = json.loads(line)
            user = data["author"]
            if user != "[deleted]":
                comment = str(data["body"])
                comment = comment.replace(",", "")
                comment = comment.replace("\n", "")
                comment = comment.replace("\r", "")
                comment = comment.replace("\t", "")

                outstring = [user + "," + comment]    
                print('Processed {} jsons'.format(counter))
                out.writerow(outstring)
                counter += 1
    counter = 0
    file_counter += 1




