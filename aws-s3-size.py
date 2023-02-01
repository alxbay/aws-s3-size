import datetime
import os
import boto3
import botocore

# DEFAULT VARS
dir_log = "out/"
s3_list = "aws-s3-size.txt"
aws_cfg = "aws.cfg"
logfile_prefix = "aws-s3-size-"
file_fulllog = logfile_prefix+"full.log"

def add_zero(s):
  res=s
  if len(s)==1:
    res = "0"+s
  return res

def str_now():
  dd = datetime.datetime.now()
  y,m,d  = str(dd.year) , add_zero(str(dd.month)) , add_zero(str(dd.day))
  ho,mi,se = add_zero(str(dd.hour)) , add_zero(str(dd.minute)) , add_zero(str(dd.second))
  return y+m+d+"_"+ho+mi+se

def load_creds(filename,service_name=""):
  res = None
  aws_access_key_id , aws_secret_access_key , aws_session_token = "","",""
  with open(filename, 'r', encoding='utf-8') as f:
    for ss in f:
      if not str(ss).startswith("#"):
        if "aws_access_key_id" in ss and "=" in ss:
          aws_access_key_id = str(ss[ss.index("=")+1:]).strip()
        if "aws_secret_access_key" in ss and "=" in ss:
          aws_secret_access_key = str(ss[ss.index("=")+1:]).strip()
        if "aws_session_token" in ss and "=" in ss:
          aws_session_token = str(ss[ss.index("=")+1:]).strip()
  if aws_access_key_id and aws_secret_access_key and aws_session_token:
    if service_name:
      res = boto3.client(
        service_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token
        )
    else:
      res = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token
        )
  return res

def get_s3_size(session,path):
  res={"size":0,"count":0}
  tmp = path.replace("s3://","")
  bucket = tmp[0:tmp.index("/")]
  folder = tmp[tmp.index("/")+1:]
  try:
    bucket = session.resource('s3').Bucket(bucket)
    for object in bucket.objects.all():
      if str(object.key).startswith(folder):
        res["size"]  += object.size
        res["count"] += 1
  except botocore.exceptions.ClientError as error:
    return None
  return res

def get_s3_from_athena(cred):
  res= []
  for i in cred.list_work_groups()["WorkGroups"]: # get workgroups from account
    s = cred.get_work_group(WorkGroup=i["Name"])
    res.append([i["Name"],s["WorkGroup"]["Configuration"]["ResultConfiguration"]["OutputLocation"]])
  return res

def get_s3_from_file(filename):
  res=[]
  with open(filename, 'r', encoding='utf-8') as f:
    for ss in f:
      if "\t" in ss:
        tmp = ss.rstrip().split("\t")
        workgroup = tmp[0]
        path = tmp[1]
      else:
        workgroup = ss
        path = ss
      res.append([workgroup,path])
  return res

aws_session = load_creds(aws_cfg)
aws_client  = load_creds(aws_cfg,"athena")
# b = get_s3_from_athena(aws_client)
b = get_s3_from_file(s3_list)
# print(b)

file_log = dir_log + logfile_prefix + str_now() + ".log"
if not os.path.exists(dir_log):
  os.makedirs(dir_log)

for i in b:
  sz = get_s3_size(session=aws_session,path=i[1])
  sz_count = -1
  sz_size  = -1
  if sz:
    sz_count = sz["count"]
    sz_size  = sz["size"]
  print(i[0],i[1],sz_size,sz_count,sep=";")
  with open(file_log, 'a', encoding='utf-8') as f:
    print(i[0],i[1],sz_size,sz_count,sep=";",file=f)
  with open(file_fulllog, 'a', encoding='utf-8') as f:
    print(str(datetime.datetime.now()).replace(" ","T"),i[0],i[1],sz_size,sz_count,sep=";",file=f)
