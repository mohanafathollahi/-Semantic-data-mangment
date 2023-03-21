import csv
import random
import string



editor_committee = ['Group_1','Group_2','Group_3']
chair_committee = ['Group_A','Group_B','Group_C']

paper_topics = ['Knowledge Discovery','machine learning', 'databases', 'natural language processing','big data']
conferences = ['Open Data Science Conference ','Women in Data Science','Healthcare NLP Summit 2022',' Gartner Data & Analytics Summit']

# Authors_Paper
with open('mydata/author_authored_by_preprocess.csv', 'r') as f:
    reader = csv.reader(f)
    article_id_author_id = {}
    for line in reader:
        line = line[0].split(';')
        article_id_author_id[line[0]]= line[1]

# Article
with open('mydata/conference_article_preprocess.csv', 'r') as f:
    reader_2 = csv.reader(f)
    #conf_article = list()
    conf_article = {}
    conf_edition = {}
    for line in reader_2:
        if len(line) != 1:
            continue
        line = line[0].split(';')
        conf_article[line[0]] = line[1]
        if len(line)<5:
            print('still have problem')
        conf_edition[line[0]] = line[6]+'-'+line[5]

# Authors
with open('mydata/authors_preprocess.csv', 'r') as f:

    reader_3 = csv.reader(f)
    author = {}
    for line in reader_3:
        line = line[0].split(';')
        author[line[0]]= line[1]

#Conference papers that accepted
with open('output/Conference_Accepted.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['Author', 'Article', 'Topic', 'Chair', 'Conference_edition','Conference_name',
                     'Reviewer1', 'Reviewer2', 'Opinion_Reviewer1', 'Opinion_Reviewer2',
                      'EventAssign','EventRev1','EventRev2'])

    for article_id in list(conf_article.keys())[:200]:
        eventassign = "EventAssign_"+(str(article_id))
        eventrev1 = "EventRev1_"+(str(article_id))
        eventrev2 = "EventRev2_"+(str(article_id))
        article_name = conf_article[article_id]
        for ar_id in article_id_author_id.keys():
            if ar_id == article_id:
                author_id = article_id_author_id[ar_id]
                for at_id in author.keys():
                    if at_id == author_id:
                        author_name = author[at_id]
        topic = random.choice(paper_topics)
        chair =random.choice(chair_committee)
        conf_edition_article = conf_edition[article_id]
        Opinion_Reviewer1 = "Description of" + ' ' + "Reviewer_1"+ ' '+ "for article:"+ ' '+ str(article_id)
        Opinion_Reviewer2 = "Description of" + ' ' + "Reviewer_2"+ ' '+ "for article:"+ ' '+ str(article_id)
        reviewer_lst =[]
        for i in range(2):
            potential_reviewer_id = random.choice(list(article_id_author_id.values()))      #chose as random one author id
            potential_reviewer_article_id = [k for k, v in article_id_author_id.items() if v == potential_reviewer_id]  #artcile id of potential reviewer

            if author_id != potential_reviewer_id and potential_reviewer_article_id != article_id:  #check if it is not one of authors
                reviewer_lst.append(author[potential_reviewer_id])
        if len(reviewer_lst) == 1:
            reviewer_lst.append("extra reviewer")
            
        writer.writerow([author_name, article_name,  topic, chair, conf_edition_article, random.choice(conferences),
                     reviewer_lst[0], reviewer_lst[1], Opinion_Reviewer1, Opinion_Reviewer2, eventassign, eventrev1, eventrev2])


#Conference papers that rejected
with open('output/Conference_rejected.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['Author', 'Article', 'Topic', 'Chair', 'Conference_edition','Conference_name',
                     'Reviewer1', 'Reviewer2', 'Opinion_Reviewer1', 'Opinion_Reviewer2',
                      'EventAssign','EventRev1','EventRev2'])

    for article_id in list(conf_article.keys())[201:250]:
        eventassign = "EventAssign_"+(str(article_id))
        eventrev1 = "EventRev1_"+(str(article_id))
        eventrev2 = "EventRev2_"+(str(article_id))
        article_name = conf_article[article_id]
        for ar_id in article_id_author_id.keys():
            if ar_id == article_id:
                author_id = article_id_author_id[ar_id]
                for at_id in author.keys():
                    if at_id == author_id:
                        author_name = author[at_id]
        topic = random.choice(paper_topics)
        chair =random.choice(chair_committee)
        conf_edition_article = conf_edition[article_id]
        Opinion_Reviewer1 = "Description of" + ' ' + "Reviewer_1"+ ' '+ "for article:"+ ' '+ str(article_id)
        Opinion_Reviewer2 = "Description of" + ' ' + "Reviewer_2"+ ' '+ "for article:"+ ' '+ str(article_id)
        reviewer_lst =[]
        for i in range(2):
            potential_reviewer_id = random.choice(list(article_id_author_id.values()))
            potential_reviewer_article_id = [k for k, v in article_id_author_id.items() if v == potential_reviewer_id]

            if author_id != potential_reviewer_id and potential_reviewer_article_id != article_id:
                reviewer_lst.append(author[potential_reviewer_id])

        writer.writerow([author_name, article_name, topic, chair, conf_edition_article, random.choice(conferences),
                     reviewer_lst[0], reviewer_lst[1], Opinion_Reviewer1, Opinion_Reviewer2, eventassign, eventrev1, eventrev2])

####################################################################################################################################

#read information of journal
with open('mydata/dblp_journal.csv', 'r') as f:
    reader_4 = csv.reader(f)
    next(reader_4)
    # conf_article = list()
    journal_names = {}
    for line in reader_4:
        line = line[0].split(';')
        journal_names[line[0]] = line[1]


#Articles published in journal
with open('mydata/journal_article_preprocess.csv', 'r') as f:
    reader_5 = csv.reader(f)
    journal_article = {}
    journal_volume = {}
    journal_id = {}
    counter = 0
    for line in reader_5:
        if len(line) != 1:
            continue
        line = line[0].split(';')
        journal_article[line[0]] = line[1]
        journal_volume[line[0]] = line[6]+'-'+line[5]
        journal_id[line[0]] = line[-1]

#journal papers that are accepted
with open('output/journal_accepted.csv', 'w', newline='') as f:
    # Columns: personID, paperID, corresponding_author
    writer = csv.writer(f)
    writer.writerow(['Author', 'Article',  'Topic', 'Volume', 'Journal_name','Editor',
    'Reviewer1', 'Reviewer2', 'Opinion_Reviewer1', 'Opinion_Reviewer2',
    'EventAssign', 'EventRev1', 'EventRev2'])

    for article_id in list(journal_article.keys())[:200]:
        eventassign = "EventAssign_"+(str(article_id))
        eventrev1 = "EventRev1_"+(str(article_id))
        eventrev2 = "EventRev2_"+(str(article_id))
        article_name = journal_article[article_id]
        for ar_id in article_id_author_id.keys():
            if ar_id == article_id:
                author_id = article_id_author_id[ar_id]
                for at_id in author.keys():
                    if at_id == author_id:
                        author_name_journal = author[at_id]
        for jr_id in journal_names.keys():
            if jr_id == journal_id[article_id]:
                jr_name = journal_names[jr_id]
        option = random.choice(paper_topics)
        editor = random.choice(editor_committee)
        journal_volume_article =  journal_volume[article_id]

        Opinion_Reviewer1 = "Description of" + ' ' + "Reviewer_1"+ ' '+ "for article:"+ ' '+ str(article_id)
        Opinion_Reviewer2 = "Description of" + ' ' + "Reviewer_2"+ ' '+ "for article:"+ ' '+ str(article_id)
        reviewer_lst = []
        for i in range(2):
            potential_reviewer_id = random.choice(list(article_id_author_id.values()))
            potential_reviewer_article_id = [k for k, v in article_id_author_id.items() if
                                             v == potential_reviewer_id]
            if author_id != potential_reviewer_id and potential_reviewer_article_id != article_id:
                reviewer_lst.append(author[potential_reviewer_id])
        if len(reviewer_lst) == 1:
            reviewer_lst.append("extra reviewer")
        
        writer.writerow([author_name_journal, article_name, option, journal_volume_article, jr_name, editor,
                         reviewer_lst[0], reviewer_lst[1],
                         Opinion_Reviewer1, Opinion_Reviewer2,  eventassign, eventrev1, eventrev2])

#journal papers that are rejected
with open('output/journal_rejected.csv', 'w', newline='') as f:
    # Columns: personID, paperID, corresponding_author
    writer = csv.writer(f)
    writer.writerow(['Author', 'Article',  'Topic', 'Volume', 'Journal_name', 'Editor',
                     'Reviewer1', 'Reviewer2', 'Opinion_Reviewer1', 'Opinion_Reviewer2',
                     'EventAssign', 'EventRev1', 'EventRev2'])

    for article_id in list(journal_article.keys())[201:250]:
        eventassign = "EventAssign_" + (str(article_id))
        eventrev1 = "EventRev1_" + (str(article_id))
        eventrev2 = "EventRev2_" + (str(article_id))
        article_name = journal_article[article_id]
        for ar_id in article_id_author_id.keys():
            if ar_id == article_id:
                author_id = article_id_author_id[ar_id]
                for at_id in author.keys():
                    if at_id == author_id:
                        author_name_journal = author[at_id]
        for jr_id in journal_names.keys():
            if jr_id == journal_id[article_id]:
                jr_name = journal_names[jr_id]
        option = random.choice(paper_topics)
        editor = random.choice(editor_committee)
        journal_volume_article = journal_volume[article_id]

        Opinion_Reviewer1 = "Description of" + ' ' + "Reviewer_1" + ' '+ "for article:"+ ' '+ str(article_id)
        Opinion_Reviewer2 = "Description of" + ' ' + "Reviewer_2" + ' '+ "for article:"+ ' '+ str(article_id)
        reviewer_lst = []
        for i in range(2):
            potential_reviewer_id = random.choice(list(article_id_author_id.values()))
            potential_reviewer_article_id = [k for k, v in article_id_author_id.items() if
                                             v == potential_reviewer_id]

            if author_id != potential_reviewer_id and potential_reviewer_article_id != article_id:
                reviewer_lst.append(author[potential_reviewer_id])

        writer.writerow(
            [author_name_journal, article_name,  option, journal_volume_article, jr_name, editor, reviewer_lst[0],reviewer_lst[1],
             Opinion_Reviewer1, Opinion_Reviewer2, eventassign, eventrev1, eventrev2])