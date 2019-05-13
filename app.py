from __future__ import print_function
import redis
import cherrypy
import datetime
import os.path
import httplib2
import zipfile
from zipfile import BadZipfile
import io
import csv


class StringGenerator(object):

    def __init__(self):
        self.browser = httplib2.Http('.cache')


    def file_checker(self,date=datetime.datetime.today()):
        path = os.path.normpath(os.path.join(os.path.dirname(__file__), 'bhavcopy'))
        if not os.path.exists(path):
            os.makedirs(path)
        filename = os.path.join(path, date.strftime('%Y-%m-%d.csv'))
        if os.path.exists(filename):
            return "pass"
        bhavcopy = self.bhavcopy(date)
        if bhavcopy is not None:
            with open(filename, 'wb') as stream:
                stream.write(bhavcopy)
            print(filename, 'OK')
            self.insert_to_redis(date)
            return "pass"
        else:
            print(filename, 'FAIL')
            return "fail"

    def insert_to_redis(self,date=datetime.datetime.today()):
        """Function to insert csv data into redis"""
        r = redis.StrictRedis(host='redis-12286.c10.us-east-1-4.ec2.cloud.redislabs.com', port=12286,
                              password='NEMSR5G7OHr5iHh90uKpau9LGS1BGbyg', decode_responses=True)
        path = os.path.normpath(os.path.join(os.path.dirname(__file__), 'bhavcopy'))
        filename = os.path.join(path, date.strftime('%Y-%m-%d.csv'))
        #Inserts csv data into redis with SC_NAME as key and all other required fields as value
        with open(filename, 'rt')as f:
            data = csv.reader(f)
            for row in data:
                stock_dict={'SC_CODE': row[0], 'SC_NAME': row[1],'OPEN':row[4],'HIGH':row[5],'LOW':row[6],'CLOSE':row[7]}
                r.hmset(row[1].replace(" ",''), stock_dict)

        with open(filename, 'rt')as f:
            data = csv.reader(f)
            #Gets all rows of csv as a list
            stock_list = [record for record in data]
        #Removes the first row which is header containing column names
        stock_list.pop(0)

        #Sort the items in the list basing on the stock closing value in descending order
        stock_list.sort(key=lambda x: float(x[7]), reverse=True)

        #Stores the top 10 stocks
        for i in range(0, 10):
            stock_dict = {'SC_CODE': stock_list[i][0], 'SC_NAME': stock_list[i][1], 'OPEN': stock_list[i][4], 'HIGH': stock_list[i][5],
                    'LOW': stock_list[i][6], 'CLOSE': stock_list[i][7]}
            r.hmset("stock" + str(i + 1), stock_dict)



    def bhavcopy(self, date):
        """Fuction to call url and fetch all csv files."""
        url = 'http://www.bseindia.com/download/BhavCopy/Equity/EQ{:%d%m%y}_CSV.ZIP'.format(date)
        response, content = self.browser.request(url)
        # If all is OK, download the zip file and extract it
        if response.status == 200:
            stream = io.BytesIO(content)
            try:
                archive = zipfile.ZipFile(stream)
            except BadZipfile:
                return None
            for filename in archive.namelist():
                return archive.open(filename).read()

        # If it's forbidden, then no Bhavcopy exists for that date.
        # Explicitly return an empty string
        elif response.status == 403:
            return ''

        # Any other status is a potential error. Just give up
        else:
            return None


    @cherrypy.expose
    def html_generator(self,search_test=''):
        r = redis.StrictRedis(host='redis-12286.c10.us-east-1-4.ec2.cloud.redislabs.com', port=12286,
                              password='NEMSR5G7OHr5iHh90uKpau9LGS1BGbyg', decode_responses=True)
        path = os.path.normpath(os.path.join(os.path.dirname(__file__), 'bhavcopy'))
        for day in range(0, 365):
            last_upload_date = datetime.date.today() - datetime.timedelta(days=day)
            filename = os.path.join(path, last_upload_date.strftime('%Y-%m-%d.csv'))
            if os.path.exists(filename):
                break
            else:
                continue

        #Variable which holds the html code
        html_out = """<html>
          <head><link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css" integrity="sha384-BVYiiSIFeK1dGmJRAkycuHAHRg32OmUcww7on3RYdg4Va+PmSTsz/K68vbdEjh4u" crossorigin="anonymous">

          <!-- Optional theme -->
          <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap-theme.min.css" integrity="sha384-rHyoN1iRsVXV4nD0JutlnGaslCJuC7uwjduW9SVrLvRYooPp2bWYgmgJQIXwl/Sp" crossorigin="anonymous"></head>
          <body>"""
        if last_upload_date==datetime.datetime.today() :
            html_out+='<h1>List of top 10 stocks today</h1>'
        else:

            html_out+='<h1>BSE hasn\'t uploaded equity file today. Displaying top 10 stocks in last uploaded file( '+str(last_upload_date.strftime('%B %d %Y'))+')</h1>'

        html_out+= '''<form action="html_generator"  method="get">
                <input type="text" name="search_test" value="" placeholder="Search Here">
                <input type="submit" class="btn btn-primary" name="" value="Submit">
            </form>
          <table class="table table-striped">
          <tr><th>Code</th><th>Name</th><th>Open</th><th>High</th><th>Low</th><th>Close</th></tr>
          '''
        #search_test gets value from the html form(search box). If its length>0 then display the user requested stock
        if len(search_test)>0:
            sc_code = r.hgetall(search_test.replace(" ",''))['SC_CODE']
            sc_name = r.hgetall(search_test.replace(" ",''))['SC_NAME']
            open = r.hgetall(search_test.replace(" ",''))['OPEN']
            high = r.hgetall(search_test.replace(" ",''))['HIGH']
            low = r.hgetall(search_test.replace(" ",''))['LOW']
            close = r.hgetall(search_test.replace(" ",''))['CLOSE']

            html_out += '<tr>' + '<td>' + sc_code + '</td>' + '<td>' + sc_name + '</td>' + '<td>' + open + '</td>' + '<td>' + high + '</td>' + '<td>' + low + '</td>' + '<td>' + close + '</td>' + '</tr>'
        # If user hasn't searched anything then display top 10 records
        else:
            for i in range(1,11):
                sc_code=r.hgetall("stock"+str(i))['SC_CODE']
                sc_name=r.hgetall("stock"+str(i))['SC_NAME']
                open = r.hgetall("stock" + str(i))['OPEN']
                high = r.hgetall("stock" + str(i))['HIGH']
                low = r.hgetall("stock" + str(i))['LOW']
                close = r.hgetall("stock" + str(i))['CLOSE']
                html_out+='<tr>'+'<td>'+sc_code+'</td>'+'<td>'+sc_name+'</td>'+'<td>'+open+'</td>'+'<td>'+high+'</td>'+'<td>'+low+'</td>'+'<td>'+close+'</td>'+'</tr>'
        html_out+='''</table></body></html>'''
        return html_out


    @cherrypy.expose
    def index(self):
        path = os.path.normpath(os.path.join(os.path.dirname(__file__), 'bhavcopy'))

        #Checks if equity file exists in BSE website with today's date
        status=self.file_checker()
        if status=='fail':
            #If no file exists with today's date, we will go back till max of 365 days to get the last uploaded file
            for i in range(1,365):
                date = datetime.date.today() - datetime.timedelta(days=i)
                filename = os.path.join(path, date.strftime('%Y-%m-%d.csv'))
                if os.path.exists(filename):
                    return self.html_generator()
                else:
                    status = self.file_checker(date)
                    if status=='fail':
                        continue
                    else:
                        return self.html_generator()
        else:
            #If file exists for today's date, get the html and display it
            return self.html_generator()
    @cherrypy.expose
    def shutdown(self):
        cherrypy.engine.exit()

if __name__ == '__main__':
    config = {
    'global': {
        'server.socket_host': '0.0.0.0',
        'server.socket_port': int(os.environ.get('PORT', 5000)),
    }
    }
    cherrypy.quickstart(StringGenerator()),'/',config=config)
