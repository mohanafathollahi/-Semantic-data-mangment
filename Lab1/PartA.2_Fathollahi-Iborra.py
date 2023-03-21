#!/usr/bin/env python

#from email import header
from neo4j import GraphDatabase
from csv import reader
from random import randint, sample, seed
import pandas as pd  


seed(5)

uri = "neo4j://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))


NUM_CITATIONS = 8
NUM_REVIEWERS = 3
KEYWORDS = ['Property Graph', 'Data modeling', 'Graph processing', 
            'Data quality', 'Data management', 'Indexing']

# CLEAN DB IN NEO4J
# MATCH (n) RETURN n LIMIT 25 -->GET GRAPH
query1 = 'MATCH (n) DETACH DELETE (n)' #clean nodes and relationships
query2 ='CALL apoc.schema.assert({}, {})' #clean contraints and indexes
with driver.session() as session:
        session.run(query1)
        session.run(query2)


#UNIQUE CONTRAINTS
unique = [('Volume', 'id'), ('Article', 'id'), ('Article', 'doi'), ('Article', 'url'),
          ('Edition', 'id'), ('Conference', 'id'), ('Author', 'id'), ('City', 'id')]

for (a, b) in unique:
    query = 'CREATE CONSTRAINT ON (x:' + a + ') ASSERT x.' + b + ' IS UNIQUE'
    with driver.session() as session:
        session.run(query)


# ARTICLE (in Journals)
# Article properties: id, title, pages, doi, url, volum, year, journal_id
# Create VOLUME node from article information: id = journal_id+volum+year (unique!)
with open('data/journal_article_preprocess.csv', 'r', encoding='ISO-8859-1') as file:
    for row in reader(file, delimiter=';'):
        journal_id = row[-1]
        article_properties = {'id': row[0], 'title': row[1], 'pages': row[2],
                   'doi': row[3], 'url': row[4], 'year':int(row[-2]), 'abstract': 'Abstract of ' + row[1]}
        volume_id = row[-1]+'-'+row[-3]+'-'+row[-2]
        volume_properties = {'id':volume_id, 'num_volume':row[-3], 'year':int(row[-2])}
        query  = 'MERGE (vo:Volume {id: $volume_id})\n'
        query += 'SET vo =  $volume_properties\n'
        query += 'MERGE (jo:Journal {id: $journal_id})\n'
        query += 'CREATE (ar:Article $article_properties)\n'
        query += 'MERGE (ar)-[:PublishedIn]->(vo)\n'
        query += 'MERGE (vo)-[:AvailableAt]->(jo)'
        with driver.session() as session:
            session.run(query, volume_id=volume_id, article_properties=article_properties, 
                        journal_id=journal_id, volume_properties=volume_properties)

#JOURNALS
with open('data/raw/dblp_journal.csv', 'r', encoding='ISO-8859-1') as file:
    reader1 = reader(file, delimiter=';')
    for row in reader1:
        Journal_id = row[0]
        journal = {'id': row[0], 'name': row[1]}
        query = 'MATCH (jo:Journal {id:$Journal_id})\n'
        query += 'SET jo= $journal'
        with driver.session() as session:
            session.run(query, journal=journal, Journal_id=Journal_id)

# ARTICLE (in Conferences)
# Article properties: id, title, pages, doi, url, volum, year, journal_id
# Create VOLUME node from article information: id = journal_id+volum+year (unique!)
with open('data/conference_article_preprocess.csv', 'r', encoding='ISO-8859-1') as file:
    for row in reader(file, delimiter=';'):
        edition_id = row[-1]+row[-2] #create unique edition id for each conferenceid-year (ONE EDITION PER YEAR)
        article_properties = {'id': row[0], 'title': row[1], 'pages': row[2],
                   'doi': row[3], 'url': row[4], 'year':int(row[-2]), 'abstract': 'Abstract of ' + row[1]}
        query = 'MERGE (ed:Edition {id: $edition_id})\n'
        query += 'CREATE (ar:Article $article_properties)\n'
        query += 'MERGE (ar)-[:PublishedIn]->(ed)'
        with driver.session() as session:
            session.run(query, edition_id=edition_id, article_properties=article_properties)


#CONFERENCES
with open('data/conferences.csv', 'r', encoding='ISO-8859-1') as file:
    reader1 = reader(file, delimiter=';')
    for row in reader1:
        conf_id = row[0]
        conference = {'id': row[0], 'name': row[1]}
        query = 'MATCH (co:Conference {id:$conf_id})\n'
        query += 'SET co= $conference'
        with driver.session() as session:
            session.run(query, conference=conference, conf_id=conf_id)

# EDITIONS
with open('data/editions.csv', 'r', encoding='ISO-8859-1') as file:
    for row in reader(file, delimiter=';'):
        edition_id = row[0]
        city_id = int(row[-3])
        city_properties = {'id':city_id, 'name':row[-2], 'country':row[-1]}
        conf_id = row[0][:9]
        edition = {'id': row[0],'begin_date': row[1], 'end_date':row[2]}
        query = 'MATCH (ed:Edition {id: $edition_id})\n'
        query += 'MERGE (ci:City {id: $city_id})\n'
        query += 'SET ci= $city_properties\n'
        query += 'MERGE (co:Conference {id: $conf_id})\n'
        query += 'MERGE (ed) -[:BelongTo]->(co)\n'
        query += 'MERGE (ed) -[:HeldIn]->(ci)\n'
        query += 'SET ed= $edition'
        with driver.session() as session:
            session.run(query, city_properties=city_properties,city_id=city_id,conf_id=conf_id,edition_id=edition_id,edition= edition)


# AUTHORS
with open('data/author_authored_by_preprocess.csv', 'r', encoding='ISO-8859-1') as file_author:
    reader1 = reader(file_author, delimiter=';')
    next(reader1)
    for row in reader1:
        Article_id = row[0]
        Author_id = row[1]
        query = 'MATCH (ar:Article {id: $Article_id})\n'
        query += 'MERGE (at: Author {id:$Author_id})\n'
        query += 'CREATE (at)-[:writes]->(ar)'
        with driver.session() as session:
            session.run(query, Article_id=Article_id, Author_id=Author_id)    

# ADD AUTHOR NAMES
with open('data/authors_preprocess.csv', 'r', encoding='ISO-8859-1') as file_author:
    reader1= reader(file_author, delimiter=';')
    next(reader1)
    for row in reader1:
        author = {'id':row[0], 'name':row[1]}
        Author_id = row[0]
        query = 'MATCH (at:Author {id:$Author_id})\n'
        query += 'SET at= $author'
        with driver.session() as session:
            session.run(query, Author_id=Author_id, author= author )   

##Get all_articles
query = 'MATCH (ar:Article)\n'
query += 'RETURN ar.id\n'
query += 'ORDER BY ar.year'
with driver.session() as session:
    all_articles = session.run(query).values()

#KEYWORDS
#Assign 2 or 3 keywords to each article randomely
for Article_id in all_articles:
    Article_id = Article_id[0]
    keywords = sample(KEYWORDS, randint(2, 3)) #get 2 or 3 keyword for each article
    for key in keywords:
        query = 'MATCH (ar:Article {id: $Article_id})\n'
        query += 'MERGE (k:Keyword {name: $key})\n'
        query += 'CREATE (ar)-[:Has]->(k)'
        with driver.session() as session:
            session.run(query, Article_id=Article_id, key=key)

#CITATIONS
articles = [a for [a] in all_articles]
for i, a in enumerate(articles):
    citations = sample(articles[:i], min(i,randint(1, NUM_CITATIONS)))
    for c in citations:
        query  = 'MATCH (ar:Article {id: $a})\n' 
        query += 'MATCH (ci:Article {id: $c})\n'
        query += 'CREATE (ar)-[:Cites]->(ci)'
        with driver.session() as session:
            session.run(query, a=a, c=c)

# CORRESPONDING AUTHOR
# Get articles and its authors
query  = 'MATCH (ar:Article)<-[:writes]-(at:Author)\n'
query += 'RETURN ar.id, at.name'
with driver.session() as session:
    result = session.run(query).values()
corresponding = {a:p for [a, p] in result} #get last author of article, correspondingset to TRUE
for a, p in corresponding.items():
    query  = 'MATCH (:Article {id: $a})<-[e:writes]-(:Author {name: $p})\n'
    query += 'SET e.corresponding = TRUE'
    with driver.session() as session:
        session.run(query, a=a, p=p)

# REVIEWERS
# Get all authors
query = 'MATCH (at:Author)\n'
query += 'RETURN at.id'
with driver.session() as session:
    authors = session.run(query).values()
all_authors = [a for [a] in authors]

article_authors = {} #dic {article : [all authors of the article] }

for [ar, au] in result:
    if ar in article_authors:
        article_authors[ar].append(au) 
    else:
        article_authors[ar] = [au] 

for article, authors in article_authors.items():
    reviewers = sample([a for a in all_authors if a not in authors], NUM_REVIEWERS)
    for r in reviewers:
        query  = 'MATCH (ar:Article {id: $article})\n'
        query += 'MATCH (at:Author {id: $r})\n'
        query += 'CREATE (at)-[:Reviews]->(ar)'
        with driver.session() as session:
            session.run(query, article=article, r=r)






