import xlrd
import json
import threading
import boto3
loc = "/Users/negi/Downloads/ISO10383_MIC.xls"

def generate_dictionary(name_of_sheet):
    workbook = xlrd.open_workbook(loc, on_demand = True)
    data = []
    try:
        worksheet = workbook.sheet_by_name(name_of_sheet) 
        first_row = [] 
        for col in range(worksheet.ncols):                                                       
            first_row.append(worksheet.cell_value(0,col))
        for row in range(1, worksheet.nrows):
            elm = {}
            for col in range(worksheet.ncols):
                elm[first_row[col]]=worksheet.cell_value(row,col)
            data.append(elm)
    except Exception, e:
        data = {"info" : str(e)}
    return json.dumps(data)

class S3Upload(object):

    def __init__(self, bucket_name, file_name, content=None, acl='private'):
        amazon_access_key = "XXXXXXXXXXXXXXXXXX"
        amazon_secret_key = "XXXXXXXXXXXXXXXXXX"
        self.acl = acl
        self.client = boto3.client('s3', aws_access_key_id=amazon_access_key, aws_secret_access_key=amazon_secret_key)
        self.bucket_name = bucket_name
        self.name = file_name
        self.content = content

    def check_exists(self):
        try:
            self.client.get_object_acl(Bucket=self.bucket_name, Key=self.name)
        except:
            return False
        return True

    def handle_upload(self):
        if self.check_exists():
            print 'ERROR:: FILE WITH THIS NAME ALREADY EXISTS!!'
        else:
            threading.Thread(group=None, target=self.upload, name=None, args=()).start()

    def upload(self):
        self.client.put_object(Bucket=self.bucket_name, Key=self.name, Body=self.content, ACL=self.acl)



if __name__ == "__main__":
    json_data = generate_dictionary("MICs List by CC")
    content = json_data
    bucket_name = "TEST_BUCKET"
    file_name = "TEST_FILE.CSV"
    file_object = S3Upload(bucket_name = bucket_name,file_name = file_name , content=content, acl = 'public-read')
    file_object.handle_upload()

