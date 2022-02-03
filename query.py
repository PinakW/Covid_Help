import pandas as pd
import getopt
import sys
from datetime import datetime
from pytz import timezone

outfile_name = ""
def display_help():
    print("If you want to search for <resource>, run\npython query.py -l <resource> -c <csv file path>\nExample:\n\tpython query.py -l concentrator -c ./Retweets.csv\n")

content_values=["bed","oxygen","medicine","remdecivir","ventilator","concentrator","icu","plasma","blood","food"]	#Keep them in lower case
try:
    opts,args=getopt.getopt(sys.argv[1:],"hl:c:p:",["help","list=","csv=","place="])
except getopt.GetoptError:
    display_help()
    sys.exit("Check your arguments after referring to help. Exitting...\n")

place=""
for opt,arg in opts:
    if opt==("-h","--help"):
        display_help()
        quit()
    elif opt in ("-l","--list"):
        resource=arg.lower()
        #print("Resource to be searched:\t" + resource)
    elif opt in ("-c","--csv"):
        path=arg
        #print("csv file being searched for:\t" + path)
    elif opt in ("-p","--place"):
        place=arg
        #print("Place to be searched for:\t" + place)
    else:
        display_help()
        quit()
if(path.lower().find("retweet") != -1):
    outfile_name = "/fac/proj_apps/pinakw/Help_for_Delhi/" + str(place) +"/Required_" + str(resource) + "_details_FromRetweets.csv"
else:
    outfile_name = "/fac/proj_apps/pinakw/Help_for_Delhi/" + str(place) +"/Required_" + str(resource) + "_details_FromTweets.csv"
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
#print(df.head(3))

#is_present = if(resource in df['Content']) return True else False
#req_df = df[is_present]

#for lst in content:
#is_present = df['Availability'] == "True"
#print(is_present.head())
#req_df = df[is_present]

istdteclmn = []
#req_df['IST_Time'] = pd.to_datetime(req_df.Time)
for dte in df['Time']:
    dte_obj = datetime.strptime(dte, '%Y-%m-%d %H:%M:%S')
    dte_obj_pacific = timezone('US/Pacific').localize(dte_obj)
    istdteclmn.append(dte_obj_pacific.astimezone(timezone('Asia/Kolkata')))

df['IST_Time'] = istdteclmn


is_present = df['Availability'] == "True"
#print(is_present.head())
req_df = df[is_present]

#req_df.reindex(index=req_df.index[::-1])
final_df = req_df[::-1]
header = ["Tweet Details","IST_Time","Tweet Location","Tweeted by","Content"]
#print(final_df.head())
final_df.to_csv(outfile_name, columns=header)
