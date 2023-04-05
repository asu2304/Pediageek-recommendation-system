# from abc import update_abstractmethods

from urllib import request
import pandas as pd
from bs4 import BeautifulSoup
import pymongo
from bson.objectid import ObjectId

# import os
# import request
# from dotenv import load_dotenv
 
# def configure():
#     load_dotenv()
    
# from decouple import config
# import os
# # Set environment variables
# os.environ['URL'] = URL

# # Get environment variables
# URL = os.getenv('URL')

if __name__ == "__main__":
    
    # configure()
    client = pymongo.MongoClient("Can't Show Database access Credentials")
    # print(client)

    db = client['test']
    blogs = db.blogs
    blogs_origional = pd.DataFrame(list(blogs.find()))
    blogs_id = blogs_origional['_id'].to_list()

    collection = db['recommending_blogs']
    
    # collection = db.recommending_blogs
    # record = {'name': 'harsh yadav', 'marks': 59}
    # collection.insert_one(record)
    # print("successfully inserted")
    # blogs_origional = pd.read_csv("blogs.csv")

blogs = blogs_origional[['_id','content','description','title']]
# blogs['content'] = str(blogs['content'])

blogs[['content']] = blogs[['content']].astype(str) 
# print(blogs['content'])

blogs['content'] = blogs[['content']].applymap(lambda text: BeautifulSoup(text, 'html.parser').get_text())

blogs['tags'] =  blogs['title'] + " " + blogs['description'] + " " + blogs['content']
blogs = blogs[['_id','tags']]

blogs[['tags']] = blogs[['tags']].astype(str)
blogs['tags'] = blogs['tags'].apply(lambda x: x.lower())
# print(blogs['tags'])

from nltk.stem.porter import PorterStemmer 
ps = PorterStemmer()

def stem(text):
    y = []
    for i in text.split():
        y.append(ps.stem(i))
    return " ".join(y)

blogs['tags'] = blogs['tags'].apply(stem)

from nltk.corpus import stopwords
import nltk
nltk.download('stopwords')
stop = stopwords.words('english')
blogs['tags'] = blogs['tags'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop)]))

from sklearn.feature_extraction.text import CountVectorizer
cv = CountVectorizer(max_features=5000, stop_words = 'english')

# read the file
file = open("updatefile.txt","r+")
num = int(file.read())
file.close()

# next 780 -> infiniy

blogs_id = blogs_id[num:]
for blog_id in blogs_id:
    
    blog_index = blogs_origional[blogs_origional['_id'] == blog_id].index[0]
    
    # print(blog_index)
    # print(blogs)
    # print(blogs['tags'])
    
    vectors = cv.fit_transform(blogs['tags']).toarray()

    vector = cv.fit_transform(blogs['tags']).toarray()[blog_index]
    from scipy import spatial
    
    vector1 = vector
    final = []

    # print(vector)
    # print(vectors)
    
    for vector in vectors:
        vector2 = vector
        result = 1 - spatial.distance.cosine(vector1, vector)
        final.append(result)

    import numpy as np
    final_np = np.array(final)

    print(f"blog num: {num}\ncurr blogs id: {blog_id}")
    num += 1
    
    def insert(final_np):
        
        recommending_list = []
        blogs_list = sorted(list(enumerate(final_np)), reverse=True, key = lambda x: x[1])[1:101]
        
        # for i in blogs_list:
        #     print(blogs_origional['_id'][i[0]])
            
        for i in blogs_list:
            recommending_list.append(blogs_origional['_id'][i[0]])
            
        # print(recommending_list)
        # record = {'_id': blog_id, 'recommending_list': recommending_list}
        # collection.insert_one(record)
        
        myquery = { "_id": blog_id }
        newvalues = {"$set":{ "recommending_list": recommending_list }}
        collection.update_many(myquery, newvalues, upsert = True)
        print("SUCCESFULLY updated")

        # collection.update_one(myquery, newvalues)
        # collection.update(myquery, newvalues)
        # collection.find_one_and_update(myquery, newvalues, upsert=True )
        # print(recommending_list)
        
    insert(final_np)
    # write the file
    
file = open("updatefile.txt","w")
file.write(f"{num}")
file.close()
    
    
    
    
