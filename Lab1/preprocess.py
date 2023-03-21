#!/usr/bin/env python

from csv import reader
from random import randint, sample, seed
import pandas as pd  

# PREPROCESSING
# Subset of articles
def subset_articles(csv1, out_file, num=100):
    '''
    Original article.csv columns:
    0: id
    -6: title
    -12: pages
    12 : doi
    -3: url
    -2: volume
    -1: year
    Output file: id, title, pages, doi, url, volume, year
    '''
    article_csv = pd.read_csv(csv1, header=None, sep=';')
    # Keep columns of interest and remove rows with missing data 
    article_csv = article_csv.iloc[:,[0,-6,-12,12,-3,-2,-1]].dropna()
    article_csv = article_csv.sample(n = num)

    return article_csv.to_csv(out_file, 
                            index=False, header=None, sep=';', float_format='%.0f')

def journal_to_article(csv1, csv2, out_file): 
    '''
    csv1: map(journal.ID <-> article.ID)
    csv2: article.csv with all information
    Add journal information to article as new column
    Output file: id, title, pages, doi, url, volum/edition, year, journal_id/conference_id
    '''
    df = pd.read_csv(csv1, sep=';')
    dict_art_journal = dict(zip(df[':START_ID'], df[':END_ID']))
    article_csv = pd.read_csv(csv2, header=None, sep=';')
    article_csv['journal_id']= article_csv[0].map(dict_art_journal)
    return article_csv.to_csv(out_file, index=False, header=None, sep=';', float_format='%.0f')
    
def create_journal_conference(csv1, out_conf, out_art, num=50):
    article_csv = pd.read_csv(csv1, header=None, sep=';')
    i0_article = article_csv.set_index(0).index
    df1 = article_csv.sample(num)
    i1_article = df1.set_index(0).index
    df2 = article_csv[~i0_article.isin(i1_article)] 
    df1.iloc[:, -1] = df1.iloc[:, -1] * 10   #redefine conference IDs
    df1.to_csv(out_conf, index=False, header=None, sep=';', float_format='%.0f')
    df2.to_csv(out_art, index=False, header=None, sep=';', float_format='%.0f')


def get_authors(csv1, csv2, csv3, out_file): 
    '''
    csv1: article_preprocess.csv 
    csv2: article-author 
    csv3: author+names
    '''
    df = pd.read_csv(csv1, header=None, sep=';')
    df1 = pd.read_csv(csv2, header=0, sep=';')
    df2 = pd.read_csv(csv3, header=0, sep=';')
    
    df1.columns = range(df1.shape[1])
    df2.columns = range(df2.shape[1])

    i_article = df.set_index(0).index
    i1_article = df1.set_index(0).index
    df1 = df1[i1_article.isin(i_article)]
    
    df1.to_csv('data/author_authored_by_preprocess.csv', index=False, header=None, sep=';', float_format='%.0f')

    i1_authors = df1.set_index(1).index
    i2_authors = df2.set_index(0).index
    df2 = df2[i2_authors.isin(i1_authors)]
    
    return df2.to_csv(out_file, index=False, header=None, sep=';', float_format='%.0f')

def create_conference_file(csv_art, out_file): #input csv
    df_art = pd.read_csv(csv_art, header=None, sep=';')
    df_out = pd.DataFrame(columns=['id','name','city', 'country'])
    num = df_art.shape[0] #number of articles in conferences
    columns = list(df_out)
    data = []
    for i in range(num):
        values = [df_art.iloc[i,-1], 'Conference_name_'+str(df_art.iloc[i,-1])] 
        zipped = zip(columns, values)
        a_dictionary = dict(zipped)
        data.append(a_dictionary)

    df_out = df_out.append(data, True)
    return df_out.to_csv(out_file, index=False, header=0, sep=';', float_format='%.0f')


def create_edition_file(csv_art, csv_city, out_file):
    df_art = pd.read_csv(csv_art, header=None, sep=';')
    df_city = pd.read_csv(csv_city , header=0)  #'data/worldcities.csv'
    df_out = pd.DataFrame(columns=['id-edition','begin_date', 'end_date', 'id-city', 'city', 'country'])
    num = df_art.shape[0] #number of articles in conferences
    columns = list(df_out)
    data = []
    for i in range(num):
        c = df_art.iloc[i,-1]  #get conference id
        y = df_art.iloc[i,-2]  #get year
        df1 = df_city.sample(1) #get one random row
        values = [str(c)+str(y), '01-01-'+str(y), '02-02-'+str(y), df1['id'].to_string(index=False),df1['city_ascii'].to_string(index=False), df1['country'].to_string(index=False)]
        zipped = zip(columns, values)
        a_dictionary = dict(zipped)
        data.append(a_dictionary)

    df_out = df_out.append(data, True)
    return df_out.to_csv(out_file, index=False, header=0, sep=';', float_format='%.0f')


seed(5)

#Subset of articles
subset_articles('data/raw/dblp_article.csv', 
                 num=1000, 
                 out_file='data/article_preprocess.csv')

# Add journal information to articles file csv as last column
journal_to_article('data/raw/dblp_journal_published_in.csv',
                    'data/article_preprocess.csv', 
                    out_file='data/article_preprocess.csv')

#Get just authors for articles subset in first step and their information. 
get_authors('data/article_preprocess.csv',
            'data/raw/dblp_author_authored_by.csv',
            'data/raw/dblp_author.csv',
            out_file='data/authors_preprocess.csv')

# Assign some articles to conferences instead of journals and modify IDs. Separate in two files (bcs of different properties)
create_journal_conference('data/article_preprocess.csv', 
                            out_conf='data/conference_article_preprocess.csv',
                            out_art='data/journal_article_preprocess.csv',
                            num=500)

## DROP VOLUME COLUMN IN CONFERENCE??? ASSUME ONE EDITION PER YEAR, THEN EDITION ID IS CONFERENCEID-YEAR

# Create artificial data for conferences.csv file
create_conference_file('data/conference_article_preprocess.csv',
                        out_file='data/conferences.csv')

create_edition_file('data/conference_article_preprocess.csv', 
                        'data/worldcities.csv', 
                        out_file='data/editions.csv')






# GENERATED FILES: ....
