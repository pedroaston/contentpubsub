import urllib3
import re
from bs4 import BeautifulSoup
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk import sent_tokenize
from nltk.corpus import stopwords
from re import findall
from xml.dom.minidom import parse
import os
import string
import json
import networkx as nx
import gensim 
from gensim.models import Word2Vec
from yattag import Doc

##################
## Get web news ##
##################
def getWebNews():
	http = urllib3.PoolManager()
	url = 'https://rss.nytimes.com/services/xml/rss/nyt/Soccer.xml'
	response = http.request('GET', url)
	soup = BeautifulSoup(response.data, 'html.parser')

	news = []

	for title in soup.find_all('title'):
		news += [title.text]

	i=0
	for description in soup.find_all('description'):
		news[i] += " "+ description.text
		i+=1

	return news

########################
## Quicksort algoritm ##
########################
def partition(array,low,high): 
    i = ( low-1 )
    pivot = array[high][1]     

    for j in range(low , high):  
        if   array[j][1] >= pivot:  
            i = i+1 
            array[i],array[j] = array[j],array[i] 

    array[i+1],array[high] = array[high],array[i+1] 
    
    return ( i+1 ) 

def quickSort(array,low,high): 
    if low < high: 
        pi = partition(array,low,high) 
        quickSort(array, low, pi-1) 
        quickSort(array, pi+1, high)

#################################################################################
## Makes the preprocessing of the document and places all n_grams(1,3) in a    ##
## list of sentences were the ngrams in the same list are in the same sentence ##
#################################################################################
def kPhrasesByNews(webNews):
	stWords = stopwords.words('english')

	wordsSent = []

	for sentence in webNews:
		aux = []
		for word in findall(r'(?u)\b[a-z]+\b',sentence.lower()):
			if word not in stWords and len(word)>1:
				aux += [word]

		wordsSent += [aux]

	kPhraseList = []

	for sentence in wordsSent:
		aux = []
		for i in range(0,len(sentence)):
			if i < len(sentence) - 2:
				aux += [sentence[i] + " " + sentence[i+1] + " " + sentence[i+2]]
			if i < len(sentence) - 1:
				aux += [sentence[i] + " " + sentence[i+1]]
			aux += [sentence[i]]

		kPhraseList += [aux]

	return kPhraseList

##############################################################
## Returns a dictionary with the prior scores for the graph ##
##############################################################
def getPriorScores(kPhraseList, webNews):
	personalizer = {}

	allKPhr = []

	for sentence in kPhraseList:
		for kPhr in sentence:
			if kPhr not in allKPhr:
				allKPhr += [kPhr]

	allNews = ""
	for news in webNews:
		allNews += news


	vectorizer = TfidfVectorizer(stop_words='english', token_pattern=r'(?u)\b[a-z]+\b', ngram_range=(1,3))
	idf_scores = vectorizer.fit_transform([allNews])

	personalizer_aux = {}

	for i in range(0, len(vectorizer.get_feature_names())):
		personalizer_aux[vectorizer.get_feature_names()[i]] = idf_scores.toarray()[0][i]

	for kPhr in allKPhr:
		if kPhr not in personalizer_aux.keys():
			personalizer[kPhr] = 0
		else:
			personalizer[kPhr] = personalizer_aux[kPhr]*(len(kPhr))

	return personalizer

##############################################################
## Get structure to provide cosine similarity to keyphrases ##
##############################################################
def getKPhrSimilarity(kPhraseList):
	return gensim.models.Word2Vec(kPhraseList,min_count = 1,size = 100,window = 5)

########################
# Get weight of edges ##
########################
def getEdgeWeights(kPhraseList):
	edgeWeights = {}

	for sentence in kPhraseList:
		for wordOrig in sentence:
			for wordDest in sentence:
				if wordOrig != wordDest:
					if wordOrig+" "+wordDest not in edgeWeights:
						edgeWeights[wordOrig+" "+wordDest] = 1
					else:
						edgeWeights[wordOrig+" "+wordDest] += 1

	return edgeWeights

######################################
## Get news context off a keyphrase ## 
######################################
def getWebNewsbyKph(webNews,kPhr):
	resNews = []
	aux = ""

	kPhr = findall(r'(?u)\b[a-z]+\b',kPhr.lower())

	for news in webNews:
		sentence = findall(r'(?u)\b[a-z]+\b',news.lower())
		for i in range(0,len(sentence)):
			if sentence[i] == kPhr[0] and len(kPhr) == 1:
				if i == 0 and i<len(sentence)-2:
					aux = kPhr[0] + " " + sentence[i+1] + " " + sentence[i+2]
				elif i > 2 and i < len(sentence)-2:
					aux = sentence[i-2] + " " + sentence[i-1] + " " + kPhr[0] + " " + sentence[i+1] + " " + sentence[i+2]
				elif i == 1 and i<len(sentence)-2:
					aux = sentence[i-1] + " " + sentence[i] + " " + sentence[i+1] + " " + sentence[i+2]
				elif i > 1 and i < len(sentence)-1:
					aux = sentence[i-2] + " " + sentence[i-1] + " " + kPhr[0] + " " + sentence[i+1]
				elif i > 1 and i < len(sentence):
					aux = sentence[i-2] + " " + sentence[i-1] + " " + kPhr[0]	
			elif len(kPhr) == 2 and i<len(sentence)-1 and sentence[i] == kPhr[0] and sentence[i+1] == kPhr[1]:
				if i == 0 and i<len(sentence)-2:
					aux = kPhr[0] + " " + kPhr[1] + " " + sentence[i+2]
				elif i > 2 and i < len(sentence)-2:
					aux = sentence[i-2] + " " + sentence[i-1] + " " + kPhr[0] + " " + kPhr[1] + " " + sentence[i+2]
				elif i == 1 and i<len(sentence)-2:
					aux = sentence[i-1] + " " + kPhr[i] + " " + kPhr[1] + " " + sentence[i+2]
				elif i == 1 and i<len(sentence)-1:
					aux = sentence[i-1] + " " + kPhr[i] + " " + kPhr[1]
				elif i > 1 and i < len(sentence)-1:
					aux = sentence[i-2] + " " + sentence[i-1] + " " + kPhr[0] + " " + kPhr[1]
				elif i > 1 and i < len(sentence)-1:
					aux = sentence[i-2] + " " + sentence[i-1] + " " + kPhr[0] + " " + kPhr[1]
			elif len(kPhr) == 3 and i<len(sentence)-2 and sentence[i] == kPhr[0] and sentence[i+1] == kPhr[1] and sentence[i+2] == kPhr[2]:
				if i == 0 and i<len(sentence)-2:
					aux = kPhr[0] + " " + kPhr[1] + " " + kPhr[2]
				elif i > 1 and i < len(sentence)-2:
					aux = sentence[i-2] + " " + sentence[i-1] + " " + kPhr[0] + " " + kPhr[1] + " " + kPhr[2]
				elif i == 1 and i<len(sentence)-2:
					aux = sentence[i-1] + " " + kPhr[i] + " " + kPhr[1] + " " + kPhr[2]
				
			if aux != "":
				resNews += [aux]
				aux = ""


	return resNews

#########################################################
## Its created the graph that is used in the pageRank  ##
## algoritm of Networkx, added the Keyphrases as nodes ##
## and edges representing their sentence neighbours    ## 
#########################################################
def getGraph(kPhraseList,edgeWeights,kPhrSimilarity):
	G = nx.Graph()

	for sentence in kPhraseList:
		for kPhr in sentence:
			G.add_node(kPhr)

	for sentence in kPhraseList:
		for wordOrig in sentence:
			for wordDest in sentence:
				if wordOrig != wordDest:
					G.add_weighted_edges_from([(wordOrig,wordDest,edgeWeights[wordOrig+" "+wordDest]+abs(kPhrSimilarity.similarity(wordOrig,wordDest)))]) 

	return G

####################################################
## Extract the scores of each keyphrase and order ## 
## them in a list, using quicksort algoritm       ## 
####################################################
def pageRankToOrderedList(pageRank):
	scoresList = []

	for kPhr in pageRank.keys():
		scoresList += [[kPhr, pageRank[kPhr]]]

	quickSort(scoresList,0,len(scoresList)-1)

	return scoresList

##################
## Main Program ##
##################
webNews = getWebNews()

kPhraseList = kPhrasesByNews(webNews)

edgeWeights = getEdgeWeights(kPhraseList)

kPhrSimilarity = getKPhrSimilarity(kPhraseList)

G = getGraph(kPhraseList,edgeWeights,kPhrSimilarity)

personalizer = getPriorScores(kPhraseList,webNews)

pageRank = nx.pagerank(G,personalization=personalizer,max_iter=50,alpha=0.15,weight='weight')

scoresList = pageRankToOrderedList(pageRank)

doc, tag, text, line = Doc().ttl()

with tag('html'):
	with tag('body'):
		text('Most relevant news Keyphrases in World Feed')
		for i in range(0,5):
			if scoresList[i][0]!='need' and scoresList[i][0]!='know':
				with tag('p'):
					text(i+1,'º: ',scoresList[i][0])
					for j in getWebNewsbyKph(webNews,scoresList[i][0]):
						with tag('p'):
							text('-    '+j) 


result = doc.getvalue()

with open('res.html','w') as f:
		f.write(result)