import requests
import json
from elasticsearch import Elasticsearch 
from datetime import datetime
from elasticsearch import helpers
import time

def create_index():
    data = '''
    {
    "mappings": {
        "properties": {
            "city": {
                "type": "text",
                "fielddata": true,
                "fields": {
                    "raw": {
                        "type": "keyword"
                    }
                }
            },
            "country": {
                "type": "text",
                "fielddata": true,
                "fields": {
                    "raw": {
                        "type": "keyword"
                    }
                }
            },
            "datetime": {
                "type": "date",
                "fielddata": true,
                "fields": {
                    "raw": {
                        "type": "keyword"
                    }
                }
            }
        }
    }
}
    '''
    try:
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        requests.put('http://localhost:9200/test_index', data=data, headers=headers)
        print("Index created!")
    except Exception as e:
        print(e)


def build_query():
    file = open('C:\\Users\\user\\Desktop\\cc.txt', 'r')
    lines = file.readlines()
    query = ''
    for line in lines:
        data = line.split("=")[1].strip()
        query = query + '{ "index" : { "_index" : "test_index" } }' + '\n'
        query = query + data + '\n'
    return query


def insert_data(data):
    try:
        print(data)
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        requests.post("http://localhost:9200/test_index/_bulk", data=data, headers=headers)
        print("Data Inserted!")
    except Exception as e:
        print(e)



if __name__ == '__main__':
    create_index()
    data = build_query()
    insert_data(data)