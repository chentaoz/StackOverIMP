
from flask import jsonify
from app import app
from cassandra.cluster import Cluster 
from flask import render_template
#r = redis.StrictRedis(host='127.0.0.1t', port=6379, db=0)
#cluster = Cluster(['ec2-52-41-16-102.us-west-2.compute.amazonaws.com'])
#session = cluster.connect('stackover')
from elasticsearch import Elasticsearch                                     		
from datetime import datetime
import json
import requests
import os                        		
es = Elasticsearch(["http://ec2-52-40-160-114.us-west-2.compute.amazonaws.com:9200"])

@app.route('/')
@app.route('/index')
def index():
  return "Hello, World!"

#@app.route('/api/posts')
#def get_posts():
       #stmt = "SELECT * FROM posts"
       #response = session.execute(stmt)
       #response_list = []
       #for val in response:
        #    response_list.append(val)
      # jsonresponse = [{"prime_id": x.prime_id, "time": x.time} for x in response_list]
       #return jsonify(jsonresponse)
#@app.route('/api/tags')
#def get_tags():
#       stmt = "SELECT * FROM tags"
#       response = session.execute(stmt)
#       response_list = []
#       for val in response:
#            response_list.append(val)
#       jsonresponse = [{"prime_id": x.prime_id, "tag_name": x.name} for x in response_list]
#       return jsonify(jsonresponse)
#@app.route('/api/posts/<tags>')
#def get_postsByTags(tags):
#      tags_list = tags.split(' ')
#      prime_list = []
#      tag_res = ""
#      for tag in tags_list:
#       	stmt = "SELECT * FROM tags where name=%s"
#       	response = session.execute(stmt, parameters=[tag])
#       	response_list = []
 #      	for val in response:
 #           	response_list.append(val)
#		print val
 #      	if len(response_list) == 0:
#		return tag+"No Found"
#       	else:
#		tag_res=tag_res+response_list[0].name
#		prime_list.append(response_list[0].prime_id)
#	
#      if len(prime_list) == 0:
#	return "No Found"
 #     else:
#	new_prime=1
#	for p in prime_list:
#		new_prime = new_prime* int(p)
#	stmt = "SELECT * FROM posts where prime_id=%s"	
 #  	print new_prime
#	response = session.execute(stmt,parameters=[str(new_prime)])
 #	response_list1 = []
  #     	for val in response:
   #             response_list1.append(val)
   #    	if len(response_list1) == 0:
 #               return "!Com! No Found"
#	
#	jsonresponse = [{"prime_id": x.prime_id, "ave_time": x.time,"tags": tag_res} for x in response_list1]
 #      	return jsonify(jsonresponse)

#@app.route('/api/test/setfoo')
#def setRedis():
#	r.set('foo', 'bar')
 #       #return r.get('foo')


 
@app.route('/home')
def home():
	return render_template("home.html")
@app.route('/tags/<t>')
def tags(t):
	res = requests.get('http://ec2-52-40-160-114.us-west-2.compute.amazonaws.com:9200/spark/tags/_search?q=tags:'+t )
	print(res.content)
	return  jsonify(res.content)
@app.route('/instead/<t>')
def instead(t):
        SITE_ROOT = os.path.realpath(os.path.dirname(__file__))
        json_url = os.path.join(SITE_ROOT, "static/data", "graph.json")
        data = json.load(open(json_url))
        #print(data)
        print(t)
        data["query"]["match"]["tags"]=t.replace("-","_")
        url = 'http://localhost:9200/spark/_graph/explore?pretty'
	#payload = json.load(open("request.json"))
	headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
        r = requests.post(url, data=json.dumps(data), headers=headers)
	return jsonify(r.json())
