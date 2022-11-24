import http.client, urllib.request, urllib.parse, urllib.error, base64
import requests
import json
import subprocess
import sys
import os
import cx_Oracle
import csv
import datetime
from dotenv import load_dotenv


load_dotenv()

lib_dir=os.getenv('ORACLE_LIB_DIR')
cx_Oracle.init_oracle_client(lib_dir=lib_dir)


class DWPush:
 ### Class that handles the connection, querying, and inserting into the OUDW. ###
    
    def __init__(self, instance='p'):
        self.instance = instance
        if(str(instance).lower() == 'p'):
            try:
                print('Creating Data Warehouse Connection....', end = '')
                dsn_tns = cx_Oracle.makedsn(os.getenv('DW_HOST'), os.getenv('DW_PORT'), service_name=os.getenv('DW_SERV'))
                conn = cx_Oracle.connect(user=os.getenv('DW_USER'), password=os.getenv('DW_PASS'), dsn=dsn_tns)
                self.connection = conn
                print('Success!')
            except Exception as e:
                print('Failed!')
                print("[Critical - Connection]: %s", str(e))
        else:
            self.connection = None

    ### Equivalent of a general query in SQL. Things like UPDATE, DELETE, TRUNC, etc. ###
    def query(self, query_str):
        """Takes in a query string and returns the result of the query in a list of lists"""
        cursor = self.connection.cursor()
        cursor.execute(query_str)
        return_list = []
        if(cursor != None):
            col_names = [row[0] for row in cursor.description]
            return_list = []
            return_list.append(col_names)
            for row in cursor:
                return_list.append(row)
            return return_list

    ### A simple way to clear the previous table entries from the target table ###
    def trunc_table(self, table):
        '''Function to truncate (clear the values from) the specified table'''
        cursor = self.connection.cursor()
        trunc_statement = 'TRUNCATE TABLE ' + table
        cursor.execute(trunc_statement)
        self.connection.commit()


    ### Equivalent of an INSERT statement in SQL. Needs target table, list of columns, and the data to be inserted. ###
    def insert(self, table, fields, data):
        '''Function to insert a dict of lists into the target table with specific fields.'''
        cur = self.connection.cursor()
        insert_table = table
        insert_fields = self.list_to_values(fields)
        insert_binds = self.list_to_binds(table)
        statement = "INSERT INTO %s(%s) VALUES (%s) " % (insert_table, insert_fields, insert_binds,)
        cur.execute(statement, data)
        self.connection.commit()

    ### Helper function to put the columns into a SQL-style format. ###
    def list_to_values(self,fields):
        '''Takes a list of fields, converts to comma-delimited string'''
        field_str = ""
        for f in fields:
            field_str += f + ","
        field_str = field_str[:-1]
        return field_str

    ### Helper function to go the other direction basically. ###
    def list_to_binds(self, table):
        '''Checks the table columns, returns fields names as comma-delimited binds'''
        columns = self.get_columns(table)
        binds = ""
        for c in columns:
            binds += ":" + c + ", "
        binds = binds[:-2] 
        return binds

    ### Equivalent of a SELECT statement in SQL. Just needs the target table. This is not general, though. ###
    def get_columns(self, table):
        columns = "SELECT column_name FROM USER_TAB_COLUMNS WHERE table_name = '" + table + "' AND column_name != 'TIMESTAMP'"
        cursor = self.connection.cursor()
        cursor.execute(columns)
        return_list = []
        for row in cursor:
            return_list.append(row[0])
        return return_list

class SkyGet:
    
    def __init__(self,giftId):
        self.giftId = giftId
        self.headers = self.header_build()
        self.params = self.params_build()

    # Builds the header from the hardcoded subscription key and a call to the authorization table in the DW for the access token
    def header_build(self):
        seg_dwp = DWPush('p')
        auth_query = os.getenv('BB_AUTH_QUERY')
        auth_list = seg_dwp.query(auth_query)
        headers = {
            'Bb-Api-Subscription-Key': os.getenv('BB_API_SUB'),
            'Authorization': 'Bearer ' + auth_list[1][0]
        }
        return headers

    # Simple builder for the additional parameters needed for the API call
    def params_build(self):
        params = urllib.parse.urlencode({
            'gift_id': self.giftId,
        })
        return params

    # Does the actual API call and prints out the result
    def sky_call(self):
        try:
            conn = http.client.HTTPSConnection('api.sky.blackbaud.com')
            conn.request("GET", "/gift/v1/gifts/{gift_id}?%s" % self.params, "{body}", self.headers)
            response = conn.getresponse()
            data = response.read()
            data = data.decode('UTF-8')
            initial = json.dumps(data, indent=4)
            result = json.loads(initial)
            return result 
            conn.close()
        except Exception as e:
            print("[Critical - GET]: %s", str(e))


def tuple_clear(tup):
    return tup[0]

def dt_format(dt):
    dt_form = "%Y-%m-%d"
    dt_trim = dt[0:10]
    dt_obj = datetime.datetime.strptime(dt_trim, dt_form)
    return dt_obj


if __name__=='__main__':
    
    # Preliminary information (Needs to be put in configuration file for obfuscation!)
    client_id = os.getenv('BB_CLIENT_ID')
    client_secret = os.getenv('BB_CLIENT_SECRET')
    authorize_url = "https://oauth2.sky.blackbaud.com/authorization"
    token_url = "https://oauth2.sky.blackbaud.com/token"
    callback_uri = "https://localhost:5000/auth/callback"
    bb_table = 'ADV_BB_ACCESS_CODE'

    # The initial authorization call to get the access code  (Write something better because this is a garbage solution!)
    authorization_redirect_url = authorize_url + '?client_id=' + client_id + '&response_type=code&redirect_uri=' + callback_uri + '&state=fud54099'
    print('\n\nFOLLOW THE LINK BELOW AND ENTER THE CODE IN THE URL AFTER AUTHORIZATION!\n\n')

    print(authorization_redirect_url)
    authorization_response = requests.get(authorization_redirect_url) 
    authorization_code = input('code: ')

    # Grabs response and loads to variable for pushing to DW
    data = {'client_id': client_id, 'client_secret': client_secret, 'grant_type': 'authorization_code', 'code': authorization_code, 'redirect_uri': callback_uri}
    headers = {'content-type': 'application/x-www-form-urlencoded'}
    access_token_response = requests.post(token_url, data=data, verify=True, allow_redirects=False, headers=headers)
    body_list = json.loads(access_token_response.text)
    
    # Pushes the authorization information to the DW for auditing and retrieval 
    insert_values = [body_list['user_id'], body_list['access_token'], body_list['refresh_token'], body_list['environment_name'], body_list['email']]
    insert_fields = ['USER_ID','ACCESS_TOKEN','REFRESH_TOKEN','ENVIRONMENT_NAME','EMAIL']
    dwp = DWPush('p')
    dwp.insert(bb_table, insert_fields, insert_values)

    # Main driver 
    counter = 1
    payment_query = os.getenv('ORACLE_PAYMENT_QUERY')
    return_sys_ids = dwp.query(payment_query)
    return_sys_ids.pop(0)
    sys_ids_list = list(map(tuple_clear, return_sys_ids))
    for i in sys_ids_list:
        seg = SkyGet(i)
        json_result = json.loads(seg.sky_call())
        result_values = [json_result['lookup_id'],json_result['linked_gifts'][0],json_result['amount']['value'],dt_format(json_result['date']),json_result['id'],json_result['type']]
        result_fields = ['GIFT_ID','ASSOCIATED_PLEDGE_ID','GIFT_AMOUNT','GIFT_DATE','GIFT_SYSTEM_ID','GIFT_TYPE']
        try:
            dwp.insert('RVW_SKY_PLEDGE', result_fields, result_values)
            print(str(counter) + ') ' + str(i) + ': Gift Loaded!')
            counter = counter + 1
        except Exception as ex:
            print(str(counter) + ') ' + str(i) + ': Gift Failed!')
            counter = counter + 1
    print('\n' + str(counter-1) + ' Objects Processed!')
