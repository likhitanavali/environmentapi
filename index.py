from flask import Flask
import time as tt
import datetime
import calendar

from werkzeug.serving import run_simple

import requests
import json
import csv
import pandas as pd
import json
from pandas.io.json import json_normalize
import re
from pygeocoder import Geocoder
import pycountry
import flatdict
import pandas as pd
import arcgis
from arcgis.gis import GIS
from IPython.display import display
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
import re
import gzip
import shutil
import requests
from math import sin, cos, sqrt, atan2, radians
app = Flask(__name__)

@app.route('/display/<latitude>,<longitude>,<time>,<param>')

def display(latitude,longitude,time,param):
    
    def Convert(tup, dic):
        for a, b in tup:
            dic.setdefault(a, []).append(b)
        return dic
    string="https://api.darksky.net/forecast/68d70a9cf38161567cc4aec124be92ed/"+latitude+","+longitude+","+time+"?exclude=hourly,currently,minutely,alert,flags"

    response = requests.get(string)
    data=response.content
    if(len(data)<140):
        return('light not available')
    parsed_data=pd.read_json(data.decode('utf-8'))

    new=parsed_data['daily'][0]
    new1=pd.DataFrame(new)
    latl=[]
    longl=[]
    for i in range(len(new1)):
        latl.append(latitude)
        longl.append(longitude)
    new1['lat']=latl
    new1['long']=longl

    gis=GIS()
    map1 = gis.map("US", zoomlevel = 8)
    df=new1
    light = gis.content.import_data(df)
    light.layer.layers = [dict(light.layer)]
    map1.add_layer(light)
    try1=light.query()
    dfn=((try1).df)
    del dfn['SHAPE']
    js=dfn.to_json(orient='index')
    parsed = json.loads(js)
    di={}
    if(param=="light"):
        new_list=[]
        for key,value in parsed["0"].items():
            if(key!="lat" and key!="long"):
                new_list.append((key,value))
        Convert(new_list, di)
        for k in list(parsed["0"].keys()):
            if k != 'lat' and k != 'long':
                del parsed["0"][k]

        parsed["lat"]=parsed["0"]["lat"]
        parsed["long"]=parsed["0"]["long"]

        del parsed["0"]
        parsed['values']=di
        lightt= (json.dumps(parsed, indent=4, sort_keys=True))
        return(lightt)
    
    #air
    response = requests.get("https://api.openaq.org/v1/measurements?coordinates="+latitude+","+longitude+"&radius=28000"+"&parameter=pm25")
    variable=str(response.content)
    if(len(variable)<140):
        return('air not available')
    xizz=variable[2:-1].encode('utf-8')
    xiz=xizz.decode('utf-8')
    st1=re.sub('"unit":".*?",', '', xiz)
    st=re.sub(',"city":".*?"}', '}', st1)
    data = json.loads(st)
    df = json_normalize(data, 'results')
    dict1=list(df['coordinates'])
    df2=pd.DataFrame(dict1)
    df['lat']=df2['latitude']
    df['long']=df2['longitude']
    del df['coordinates']
    dflist = df['date'].tolist()
    l_data=[]
    l_local=[]
    for i in dflist:
        l_data.append(i['local'][:10])
    for i in dflist:
        l_local.append(i['local'][20:])
    del df['date']
    del df['location']
    df4 = pd.DataFrame()
    df4['dates']=l_data
    df4['local']=l_local
    df['date']=df4['dates']
    df['local']=df4['local']
    epoch=[]
    for i in range(len(df)):
        date_time = df['date'][i]
        t = int(tt.mktime(tt.strptime(str(df['date'][i]), "%Y-%m-%d")))
        epoch.append(t)
    df['epocht']=epoch
    df.to_csv("final.csv")
    gis=GIS()
    map1 = gis.map("Amsterdam", zoomlevel = 8)
    df = pd.read_csv('final.csv')
    airpol = gis.content.import_data(df)
    airpol.layer.layers = [dict(airpol.layer)]
    map1.add_layer(airpol)
    try1=airpol.query().df
    del try1['SHAPE']
    del try1['Unnamed__0']
    time1=0
    time1=int(time)
    min1=99999999999
    key1=0
    for i in range(len(try1['epocht'])):
        if((abs(int(try1['epocht'][i])-time1))<=min1):
            min1=abs(int(try1['epocht'][i])-time1)
            key1=i
            if(min1==0):
                break
    try2=try1.loc[key1]
    js2=try2.to_json(orient='index')
    parsed2=json.loads(js2)
    if(param=="air"):
        new_list2=[(parsed2['parameter'],parsed2['value'])]
        di2= {}
        Convert(new_list2, di2)
        del parsed2['parameter']
        del parsed2['value']
        parsed2['values']=di2
        airr= (json.dumps(parsed2, indent=4, sort_keys=True))
        return(airr)
    if(param=="airlight"):
        your_json=(str(js2)+str(js))
        new=your_json.split("}")
        air=new[0]+"}"
        light=new[1]+"}"+"}"
        parsed2 = json.loads(air)
        new_list2=[(parsed2['parameter'],parsed2['value'])]
        di2= {}
        Convert(new_list2, di2)
        del parsed2['parameter']
        del parsed2['value']
        parsed2['values']=di2
        parsed = json.loads(light)
        new_list=[]
        for key, value in parsed["0"].items():
            if(key!="lat" and key!="long"):
                new_list.append((key,parsed["0"][key]))
        di= {}
        Convert(new_list, di)
        for k in list(parsed["0"].keys()):
            if k != 'lat' and k != 'long':
                del parsed["0"][k]
        parsed["lat"]=parsed["0"]["lat"]
        parsed["long"]=parsed["0"]["long"]
        del parsed["0"]
        data = {}
        data.update(di) 
        data.update(di2)
        parsed['values']=data
        return (json.dumps(parsed, indent=4, sort_keys=True))
    #viirs\
    
    req = Request("https://ngdc.noaa.gov/eog/viirs/download_flare_only_iframe.html")
    html_page = urlopen(req)

    soup = BeautifulSoup(html_page, "lxml")

    links = []
    for link in soup.findAll('a'):
        links.append(link.get('href'))

    #print(links)
    sub = 'd201712'
    links_dec=[]
    links_dec=([s for s in links if sub in s])
    #print(links_dec)
    sub = 'only.csv.gz'
    links_dec_csv=[]
    links_dec_csv=([s for s in links_dec if sub in s])
    url =links_dec_csv[0]
    filename = "latest.csv.gz"
    with open(filename, "wb") as f:
        r = requests.get(url)
        f.write(r.content)
    
    with gzip.open('VNF_npp_d20171205_noaa_v21.flares_only.csv.gz', 'rb') as f_in:
        with open('latest.csv', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    chunksize = 10 ** 6
    viirs=pd.DataFrame()
    for chunk in pd.read_csv("latest.csv", chunksize=chunksize):
        viirs=viirs.append(chunk)
    dif=1000
    a=0
    b=0
    k=999999
    for i in range (len(viirs)):
        R = 6373.0
        lat1 = radians(float(viirs['Lat_GMTCO'][i]))
        lon1 = radians(float(viirs['Lon_GMTCO'][i]))
        lat2 = radians(float(latitude))
        lon2 = radians(float(longitude))
        dlon = lon2 - lon1
        dlat = lat2 - lat1

        a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))

        distance = R * c
        #print(str(i)+","+str(a+b))
        if(distance<=dif):
            dif=distance
            k=i
    if(k==999999):
        return("data does'nt exist")
    viirsdf=viirs.loc[[k]]
    viirsdf1=viirsdf
    viirsdf1['latitude']=latitude
    viirsdf1['longitude']=longitude
    virlight=gis.content.import_data(viirsdf1)
    virlight.layer.layers = [dict(virlight.layer)]
    map1.add_layer(virlight)
    virdf=virlight.query()
    dfn=((virdf).df.head())
    js3=dfn.to_json(orient='index')
    if(param=='viirs'):
        return(js3)
if __name__=='__main__':
	run_simple('localhost',9000, app)
