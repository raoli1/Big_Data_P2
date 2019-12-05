import gensim

model = gensim.models.Word2Vec.load('mymodel')
result = model.most_similar('true', topn=10)

print(result)