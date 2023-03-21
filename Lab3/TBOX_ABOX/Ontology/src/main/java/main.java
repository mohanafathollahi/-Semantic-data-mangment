import org.apache.jena.ontology.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDF;

import java.io.*;

public class main {
    public static void main(String[] args) throws IOException{
        String sdm = "http://www.sdm.com/";

        OntModel m = ModelFactory.createOntologyModel( OntModelSpec.RDFS_MEM_RDFS_INF);

        // First create Person Class and its subclasses
        OntClass person = m.createClass(sdm + "Person");
        OntClass chair = m.createClass(sdm + "Chair");
        OntClass editor = m.createClass(sdm + "Editor");
        OntClass author = m.createClass(sdm + "Author");
        OntClass reviewer = m.createClass(sdm + "Reviewer");
        person.addSubClass(chair);
        person.addSubClass(editor);
        person.addSubClass(author);
        person.addSubClass(reviewer);

        // Now Venues Class and subclasses
        OntClass venue = m.createClass(sdm + "Venue");
        OntClass conference = m.createClass(sdm + "Conference");
        OntClass journal = m.createClass(sdm + "Journal");
        OntClass workshop = m.createClass(sdm + "Workshop");
        OntClass symposium = m.createClass(sdm + "Symposium");
        OntClass expertgroup = m.createClass(sdm + "ExpertGroup");
        OntClass regularconference = m.createClass(sdm + "RegularConference");
        venue.addSubClass(conference);
        venue.addSubClass(journal);
        conference.addSubClass(workshop);
        conference.addSubClass(expertgroup);
        conference.addSubClass(symposium);
        conference.addSubClass(regularconference);

        // Proceedings and volume classes
        OntClass proceedings = m.createClass(sdm + "Proceedings");
        OntClass volume = m.createClass(sdm + "Volume");

        //Paper Class and subclasses
        OntClass paper = m.createClass(sdm + "Paper");
        OntClass journalpaper = m.createClass(sdm + "JournalPaper");
        OntClass conferencepaper = m.createClass(sdm + "ConferencePaper");

        OntClass fullpaper = m.createClass(sdm + "FullPaper");
        OntClass shortpaper = m.createClass(sdm + "ShortPaper");
        OntClass demopaper = m.createClass(sdm + "DemoPaper");
        OntClass posterpaper = m.createClass(sdm + "PosterPaper");
        OntClass conferencepublication = m.createClass(sdm + "ConferencePublication");
        OntClass journalpublication = m.createClass(sdm + "JournalPublication");

        // Event Classes
        OntClass eventassign = m.createClass(sdm + "EventAssigsdm");
        OntClass eventreview = m.createClass(sdm + "EventReviews");

        paper.addSubClass(journalpaper);
        paper.addSubClass(conferencepaper);

        journalpaper.addSubClass(fullpaper);
        journalpaper.addSubClass(shortpaper);
        journalpaper.addSubClass(demopaper);
        conferencepaper.addSubClass(fullpaper);
        conferencepaper.addSubClass(shortpaper);
        conferencepaper.addSubClass(demopaper);
        conferencepaper.addSubClass(posterpaper);

        journalpaper.addSubClass(journalpublication);
        conferencepaper.addSubClass(conferencepublication);

        // Topic class
        OntClass topic = m.createClass(sdm + "Topic");

        // Review Class
        OntClass review = m.createClass(sdm + "Review");

        // PROPERTIES
        // Author write a paper
        OntProperty writes = m.createOntProperty(sdm + "Writes");
        writes.addDomain(author);
        writes.addRange(paper);

        // All papers can be submitted to conferences
        OntProperty submittedtoconference = m.createOntProperty(sdm + "SubmittedToConference");
        submittedtoconference.addDomain(conferencepaper);
        submittedtoconference.addRange(conference);

        // JournalPapers are submitted to journals
        OntProperty submittedtojournal = m.createOntProperty(sdm + "SubmittedToJournal");
        submittedtojournal.addDomain(journalpaper);
        submittedtojournal.addRange(journal);

        // Reviewer accepts or rejects in an event
        OntProperty reviews = m.createOntProperty(sdm + "Reviews");
        reviews.addDomain(eventreview);        //
        reviews.addRange(reviewer);

        // Papers are accepted or rejected by an reviewing event
        OntProperty accepts = m.createOntProperty(sdm + "Accepts");
        accepts.addDomain(eventreview);         //source
        accepts.addRange(journalpublication);  //destination
        accepts.addRange(conferencepublication);


        OntProperty rejects = m.createOntProperty(sdm + "Rejects");
        rejects.addDomain(eventreview);
        rejects.addRange(paper);

        // Reviewing events have a backup text
        OntProperty writereview = m.createOntProperty(sdm + "WritesReview");
        writereview.addDomain(eventreview);
        writereview.addRange(review);

        // Editor handles Journal
        OntProperty handlejournal = m.createOntProperty(sdm + "HandleJournal");
        handlejournal.addDomain(editor);
        handlejournal.addRange(journal);

        // Chair handles conference
        OntProperty handleconference = m.createOntProperty(sdm + "HandleConference");
        handleconference.addDomain(chair);
        handleconference.addRange(conference);

        // Chairs and Editors assign reviewers in an event
        OntProperty assignedby = m.createOntProperty(sdm + "AssignedBy");
        assignedby.addDomain(eventassign);
        assignedby.addRange(chair);
        assignedby.addRange(editor);

        // Reviewers are assigned in an event
        OntProperty assigsdm = m.createOntProperty(sdm + "Assigsdm");
        assigsdm.addDomain(eventassign);
        assigsdm.addRange(reviewer);

        // Assignatiosdm are done to a paper
        OntProperty assignedto = m.createOntProperty(sdm + "AssignedTo");
        assignedto.addDomain(eventassign);
        assignedto.addRange(paper);

        // Volumes belong to Journal
        OntProperty belongstojournal = m.createOntProperty(sdm + "BelongsToJournal");
        belongstojournal.addRange(journal);
        belongstojournal.addDomain(volume);

        // Proceedings are editiosdm of Conferences
        OntProperty editionofconference = m.createOntProperty(sdm + "EditionOfConference");
        editionofconference.addRange(conference);
        editionofconference.addDomain(proceedings);

        // Publicatiosdm are published in volumes
        OntProperty publishedinvolume = m.createOntProperty(sdm + "PublishedInVolume");
        publishedinvolume.addRange(volume);
        publishedinvolume.addDomain(journalpublication);

        // Publicatiosdm are published in edition
        OntProperty publishedinedition = m.createOntProperty(sdm + "PublishedInEdition");
        publishedinedition.addRange(proceedings);
        publishedinedition.addDomain(conferencepublication);

        // Papers and Venues are related with Topics
        OntProperty relatedTo = m.createOntProperty(sdm + "RelatedTo");
        relatedTo.addDomain(paper);
        relatedTo.addDomain(venue);
        relatedTo.addRange(topic);

        // Save XML File
        String fileName = "ontologies/TBOX.ttl";
        FileWriter out = new FileWriter( fileName );
        m.write( out, "turtle" );
        out.close();
        //throw new IOException("Done!")

        InputStream in2 = RDFDataMgr.open("XML TTL Data/Conference-rejected-csv.rdf");
        InputStream in3 = RDFDataMgr.open("XML TTL Data/Conference-Accepted-csv.rdf");
        InputStream in4 = RDFDataMgr.open("XML TTL Data/journal-accepted-csv.rdf");
        InputStream in5 = RDFDataMgr.open("XML TTL Data/journal-rejected-csv.rdf");

        m.read(in2, null);
        m.read(in3, null);
        m.read(in4, null);
        m.read(in5, null);

        int min = 0;
        int max = 4;
        // Randomly set type of Paper
        ResIterator iter_0 = m.listSubjectsWithProperty(submittedtoconference);
        while (iter_0.hasNext()) {
            Resource r = iter_0.nextResource();
            int random_int = (int) Math.floor(Math.random()*(max-min+1)+min);
            if ( random_int == 0) {
                r.addProperty(RDF.type, fullpaper);
            } else if (random_int == 1) {
                r.addProperty(RDF.type, shortpaper);
            } else if (random_int == 2) {
                r.addProperty(RDF.type, demopaper);
            } else  {
                r.addProperty(RDF.type, posterpaper);
            }
        }

        // Randomly assign type of conference
        NodeIterator iter_1 = m.listObjectsOfProperty(editionofconference);
        while (iter_1.hasNext()) {
            Resource r = iter_1.nextNode().asResource();
            int random_int = (int) Math.floor(Math.random() * (max - min + 1) + min);
            if (random_int == 0) {
                r.addProperty(RDF.type, workshop);
            } else if (random_int == 1) {
                r.addProperty(RDF.type, symposium);
            } else if (random_int == 2) {
                r.addProperty(RDF.type, expertgroup);
            } else {
                r.addProperty(RDF.type, regularconference);
            }
        }


        ResIterator iter_2 = m.listSubjectsWithProperty(submittedtojournal);
        while (iter_2.hasNext()) {
            Resource r = iter_2.nextResource();
            int random_int = (int) Math.floor(Math.random() * (max - min + 1) + min);
            if ( random_int == 0) {
                r.addProperty(RDF.type, fullpaper);
            } else if (random_int == 1) {
                r.addProperty(RDF.type, shortpaper);
            } else {
                r.addProperty(RDF.type, demopaper);
            }
        }


        String fileName2 = "ontologies/ABOX_TBOX_RDFS_2.ttl";
        FileWriter out2 = new FileWriter( fileName2 );
        m.write( out2, "turtle" );
        out2.close();

    }
}





