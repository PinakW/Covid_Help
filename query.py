import pandas as pd
import getopt
import sys

def display_help():
    print("If you want to search for <resource>, run\npython query.py -l <resource> -c <csv file path>\nExample:\n\tpython query.py -l concentrator -c ./Retweets.csv\n")

content_values=["bed","oxygen","medicine","remdecivir","ventilator","concentrator","icu","plasma","blood","food"]	#Keep them in lower case
try:
    opts,args=getopt.getopt(sys.argv[1:],"hl:c:",["help","list=","csv="])
except getopt.GetoptError:
    display_help()
    sys.exit("Check your arguments after referring to help. Exitting...\n")


for opt,arg in opts:
    if opt==("-h","--help"):
        display_help()
        quit()
    elif opt in ("-l","--list"):
        resource=arg.lower()
        print("Resource to be searched:\t" + resource)
    elif opt in ("-c","--csv"):
        path=arg
        print("csv file being searched for:\t" + path)

df = pd.read_csv(path)
text = df['Tweet Details']
content=[]
content_val="NULL"
resource_clmn = []
resource_avail=""
for line in text:
    content_val=[x for x in content_values if x.lower() in line.lower()]
    content.append(content_val)
#    resource_avail = [resource in content_val]
    if resource in content_val:
        resource_avail = 'True'
    else:
        resource_avail = 'False'
    resource_clmn.append(resource_avail)
#print(content)
df['Content'] = content
df['Availability'] = resource_clmn
print(df.head(3))

#is_present = if(resource in df['Content']) return True else False
#req_df = df[is_present]

#for lst in content:
is_present = df['Availability'] == "True"
#print(is_present.head())
req_df = df[is_present]
print(req_df.head())
req_df.to_csv("Required_details.csv")
