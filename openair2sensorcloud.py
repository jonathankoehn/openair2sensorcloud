import json
import requests
import pandas
import uuid

# URL OpenAir-API:
baseurlOpenair = 'http://openair-api.datacolonia.de'

# URLs SensorCloud:
urlDal = '<URL-DAL>'
urlLast = '<URL-Last>'

# Sensors (key: OpenAir-ID, value: SensorCloud-ID)
senids = {}
senids['F686D6'] = '088565bd-b46a-4843-816b-ee1ea9253794'

senids['F686BA'] = '0e030e7e-f2e3-47fc-b0c9-afe207220326'
senids['F686E2'] = 'a9d44a56-5ba0-4254-89c3-1746f0774798'
senids['F68735'] = '3de797c8-0a6d-41d5-b4a9-01e5df0993e4'
senids['F68654'] = '0390218d-beec-49c7-8161-87a8e1b2795f'
senids['F685E9'] = '0fa6e5c6-27e2-4e4e-be84-f0889dfa6a89'
senids['F686D9'] = '0c141aee-f669-494d-941f-0fe77d9bf698'
senids['CA2898'] = 'bd8eec5c-94ac-4e5a-8065-cf3c6463f340'
senids['CA2766'] = '8565b46d-af24-406c-8c38-a34999f1dbdb'
senids['CA28DB'] = '8db25199-9735-4783-90d4-d5b8e16b5243'
senids['F686D0'] = '3ff9def1-4199-452e-9936-a392fe34cf7d'

measurements = {}
measurements['r_no'] = 'R1'
measurements['r_co'] = 'R2'

# Get json-object from HTTP-API
def getJson(measurement, sensorid):
    timeLast = getLastTime (senids[sensorid], measurements[measurement])
    
    selectstring = 'SELECT value FROM open_air..'+ measurement +' WHERE time > '+ timeLast +'ms AND id=\'' + sensorid + '\''
    url = baseurlOpenair + '/?q=' + selectstring
    print(url)
    r = requests.get(url)
    r_statuscode = r.status_code
    r_contenttype = r.headers['content-type']
    r_encoding = r.encoding
    r_text = r.text

    # exit, if wrong response code 
    if(r_statuscode != 200):
        print('Ungueltige Antwort. HTTP-Responsecode:', r_statuscode)
        print('Antwort ist vom Typ', r_contenttype)
        print('Inhalt:', r_text)
        exit()
    # exit, if wront content type
    if r_contenttype != 'application/json':
        print('Kein JSON-Objekt gefunden. Contenttype ist:', r_contenttype)
        print('Inhalt:', r_text)
        exit()
    else:
        # return json-objekt
        r_json = r.json()
        return r_json

# Sends sensordata-messages of json-object to SensorClouds DAL-service. One message per value, as I don't trust the DAL.
def insertFromJson(source_dict, senid, mwname):
    for value in source_dict.get('results')[0].get('series')[0].get('values'):
        ts_source = value[0]
        source_val = value[1]
        
        # Calculate timestamps
        ts_measurement = pandas.to_datetime(ts_source) - pandas.to_datetime('1970-01-01')
        ts_now = ( pandas.Timestamp('now') - pandas.to_datetime('1970-01-01') )
        
        ts_t_string = "%.0f" % (1000* (ts_measurement-ts_now).total_seconds() )
        ts_now_string = "%.0f" % (1000* (ts_now).total_seconds() )
        
        n_string = str(uuid.uuid1())+':'+mwname
        
        # Prepare sensordata message
        e = {}
        e['n'] = n_string
        e['t'] = ts_t_string
        e['sv'] = str(source_val)

        payload = {}
        payload['typ'] = '1'
        payload['gw'] = 'abbd5ecf-1b69-4ccc-bb99-a4ce34915a97'
        payload['bn'] = senid
        payload['bt'] = ts_now_string
        payload['e'] = []
        payload['e'].append(e)
        
        payload_json=json.dumps(payload, indent=8)

        #print(payload_json)
        r = requests.put(urlDal, data=payload_json, headers={'content-type':'json'})
        if r.status_code != 201:
            print('Erhalte unerwartete Rueckgabe bei', senid,)
            print(r)

# returns the latest timestamp for a given sensor and measurement
def getLastTime(senid, measurement):
    print(urlLast+senid)
    r = requests.get(urlLast+senid)
    
    if r.status_code != 200:
        return '0'
    
    r_json = r.json()
    print(r_json)
    for entry in r_json:
        if entry.get('n').split(':')[1] == measurement:
            t = entry.get('t')
            print('letzter Eintrag fuer ', senid, 'und', measurement, 'gefunden. t:',t)
            return t
    return '0'

def run():
    for m in measurements.keys():
        for sid in senids.keys():
            json = getJson(m, sid)
            # Skip empty objects
            if json.get('results')[0].get('series') == None:
                print('Verwerfe leeres Objekt von', sid)
                continue
            # Skip, if object has not columns [0]: time, [1]: value
            if json.get('results')[0].get('series')[0].get('columns')[0] != 'time' or json.get('results')[0].get('series')[0].get('columns')[1] != 'value':
                print('Objekt von ', sid, 'hat nicht den erwarteten Spaltenaufbau')
                continue
            # Perform insert
            insertFromJson(json, senids[sid], measurements[m])

run()
