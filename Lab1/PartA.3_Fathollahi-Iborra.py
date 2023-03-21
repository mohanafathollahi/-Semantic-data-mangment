#!/usr/bin/env python

from neo4j import GraphDatabase
import random
from random import randint, sample, seed

seed(5)

uri = "neo4j://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

unique = [('University', 'name'), ('Company', 'name')]

for (a, b) in unique:
    query = 'CREATE CONSTRAINT ON (x:' + a + ') ASSERT x.' + b + ' IS UNIQUE'
    with driver.session() as session:
        session.run(query)


# ADD AFFILIATION TO AUTHORS

UNIVERSITY = ['UPC','USF','STANFORD','BERKELEY','MUNICH']
COMPANY = ['BSC','AMAZON','APPLE','MICROSOFT','TOYOTA']

query = 'MATCH (at:Author)\n'
query += 'RETURN at.id'
with driver.session() as session:
    authors = session.run(query).values()
all_authors = [a for [a] in authors]

num =  len(all_authors)//2

uni_authors = random.sample(all_authors,num)

for i, author_id in enumerate(all_authors):
    if author_id in uni_authors:
        university = random.sample(UNIVERSITY, 1)
        query = 'MATCH (at:Author {id: $author_id})\n'
        query += 'MERGE (uni:University {name: $university})\n'
        query += 'MERGE (at)-[:AffiliatedTo]->(uni)'
        with driver.session() as session:
            session.run(query, author_id=author_id, university=university)
    else:
        company = random.sample(COMPANY, 1)
        query = 'MATCH (at:Author {id: $author_id})\n'
        query += 'MERGE (com:Company {name: $company})\n'
        query += 'MERGE (at)-[:AffiliatedTo]->(com)'
        with driver.session() as session:
            session.run(query, author_id=author_id, company=company)

## ADD REVIEWERS COMMENTS AND ACCEPTANCE

query  = 'MATCH (at:Author)-[:Reviews]->(ar:Article)\n'
query += 'RETURN ar.id, at.name'
with driver.session() as session:
    result = session.run(query).values()

art_reviewers = {} #dic article: authors reviewers of the article
for [ar, au] in result:
    if ar not in art_reviewers:
        art_reviewers[ar] = [au]
    else:
        art_reviewers[ar].append(au)

for art, reviewers in art_reviewers.items():
    for i, author in enumerate(reviewers): 
        review = {'Comment': 'Reviewer ' + author + ' comment on ' + art, 'Acceptance': i%3 != 0} #ensure 2 out of 3 accepts
        query  = 'MATCH (ar:Article {id: $art})<-[r:Reviews]-(at:Author {name: $author})\n'
        query += 'SET r = $review'
        with driver.session() as session:
            session.run(query, art=art, author=author, review=review)