#Name : Team4(Myat Oo,Zijian Zhang,Mohammad Islam
#Title: queries.py
#Date : March 1, 2017
#Assignment: Assignment_1
#Purpose   : In this project, we work with MongoDB importing the JSON data and write queries for the following questions
#           in the MongoDB and then write the following queries again using Pymongo.
            
            # a)    Find the total number of cities in the database.
            # b)    Create the list of states, cities, and city populations.
            # c)    List the cities in the state of Massachusetts with populations between 1000 and 2000.
            # d)    Write a mapReducer to compute the total number of cities and total population in each state.
            # e)    Write a mapReducer to find the average city population for each state.
            # f)    Write a mapReducer to find the least densely-populated state(s).
            


from pymongo import MongoClient  #import MongoClient
from bson.code import Code       #import Code 
client = MongoClient('127.0.0.1', 27017)

db = client["Project1"] #database name

collection = db["zipcodes"] # collection name

#query_a 
print ("A. Find the total number of cities in the database")

result1=collection.distinct("city")
i = 0
for doc1 in result1:
   i=i+1
print (i)


#query_b
print ("B. Create the list of states, cities, and city populations.")

result2=collection.aggregate([{ "$group":{"_id":{'state': '$state','city': '$city','pop':'$pop','z':'_id' }}},{"$group":{"_id":{'state':'$_id.state','city':'$_id.city','pop':'$_id.pop'}}}])
for doc2 in result2:
   print(doc2)
    
#query_c 
print ("C. List the cities in the state of Massachusetts with populations between 1000 and 2000.")

result3 =collection.distinct('city',{'state':'MA','pop':{'$gte':1000,'$lte':2000}})
for doc3 in result3:
   print (doc3)

#query_d 
print("D. Write a mapReducer to compute the total number of cities and total population in each state.")

def computeTotalNumAndTotalPop(mongodb):
   map= Code(""" function() {emit(this.city, this.pop);};""")
   reduce = Code("""function(k, v) {return Array.sum(v);return Array.sum(k);};""")
   keys = collection.map_reduce(map,reduce, "map_reduce_Done") 
   return keys.find()
   
result4 =computeTotalNumAndTotalPop(db)
for doc4 in result4:
   print(doc4)
#query_e
print("E. Write a mapReducer to find the average city population for each state.")

def avgCityPopSate(mongodb):
   map = Code("""function() {emit(this.state, this.pop);};""")
   reduce = Code("""function(k, v) {return Array.avg(v);};""")
   keys = collection.map_reduce(map,reduce, "map_reduce_Done")
   return keys.find()
 
result5 =avgCityPopSate(db) 
for doc5 in result5:
   print(doc5)

#query_f
print("F. Write a mapReducer to find the least densely-populated state(s).")

def leastDenselyPop(mongodb):
   map = Code("""function() {emit(this.state, this.pop);};""")
   reduce = Code("""function(k, value) {return Array.sum(value);};""")
   keys = collection.map_reduce(map,reduce,"map_reduce_Done")
   return keys.find().sort("value",1).limit(1)

result6 = leastDenselyPop(db)
for doc6 in result6:
   print(doc6)
