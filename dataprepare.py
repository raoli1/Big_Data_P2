import pymongo
from pymongo import MongoClient
import json
import pandas as pd
from nltk.corpus import stopwords
import re
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tokenize import word_tokenize
from nltk.stem.snowball import SnowballStemmer
import gensim

def connectToMongo():
    
    #connect to MongoDB
    client = pymongo.MongoClient("mongodb+srv://raoli:Wmcy941013@cluster0-eswl9.azure.mongodb.net/test?retryWrites=true&w=majority")
    return client

def importDatabase():
    #convert csv file to json
    data = pd.read_csv("https://raw.githubusercontent.com/raoli1/Big_Data_P2/test/IMDB_Dataset.csv")
    data_json = json.loads(data.to_json(orient='records'))

    #connect to MongoDB
    client = connectToMongo()
    db = client.IMDB_Movie_Reviews
    collection = db.reviews
    #avoid duplicate data
    collection.remove()
    collection.insert(data_json)

def rem_sw(df):
    # Downloading stop words
    stop_words = set(stopwords.words('english'))

    # Removing Stop words from training data
    count = 0
    for sentence in df:
        sentence = [word for word in sentence.lower().split() if word not in stop_words]
        sentence = ' '.join(sentence)
        df[count] = sentence
        count+=1
    return(df)

def rem_punc(df):
    count = 0
    for s in df:
        cleanr = re.compile('<.*?>')
        s = re.sub(r'\d+', '', s)
        s = re.sub(cleanr, '', s)
        s = re.sub("'", '', s)
        s = re.sub(r'\W+', ' ', s)
        s = s.replace('_', '')
        df[count] = s
        count+=1
    return(df) 

def lemma(df):
    result = []
    lmtzr = WordNetLemmatizer()

    #count = 0
    stemmed = []
    for sentence in df:    
        word_tokens = word_tokenize(sentence)
        for word in word_tokens:
            stemmed.append(str(lmtzr.lemmatize(word)))
        #sentence = ' '.join(stemmed)
        result.append(stemmed)
        #count+=1
        stemmed = []
    return(result)
'''
def stemma(df):
    result = []
    stemmer = SnowballStemmer("english") #SnowballStemmer("english", ignore_stopwords=True)

    #count = 0
    stemmed = []
    for sentence in df:
        word_tokens = word_tokenize(sentence)
        for word in word_tokens:
            stemmed.append(str(stemmer.stem(word)))
        #sentence = ' '.join(stemmed)
        result.append(stemmed)
        #count+=1
        stemmed = []
    return(result)

'''

client = connectToMongo()
db = client.IMDB_Movie_Reviews
collection = db.reviews

df = pd.DataFrame(list(collection.find()))['review']
df = rem_sw(df)
df = rem_punc(df)
result = lemma(df)
print (result[0:10])
model = gensim.models.Word2Vec(result, min_count=10)
model.save('mymodel')

'''
df_complete['review'] = df
df_complete_json = df_complete.to_json(orient='records')
#connect to MongoDB
client = connectToMongo()
db = client.IMDB_Movie_Reviews
collection = db.reviews_processed
#avoid duplicate data
collection.remove()
collection.insert(df_complete_json)
'''


