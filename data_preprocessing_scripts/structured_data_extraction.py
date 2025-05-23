import requests
import psycopg2
import json
import os 
from dotenv import load_dotenv

load_dotenv()

# Verbindung zur PostgreSQL-Datenbank herstellen
conn = psycopg2.connect(
    host="localhost",
    dbname="ratsinformationssystem_structured_250129",
    user=os.environ.get("DB_USER"),
    password=os.environ.get("DB_PASSWORD"),
    port="5432"
)

link_person = "https://web1.karlsruhe.de/ris/oparl/bodies/0001/people"
link_membership = "https://web1.karlsruhe.de/ris/oparl/bodies/0001/memberships"
link_organization = "https://web1.karlsruhe.de/ris/oparl/bodies/0001/organizations"
link_meeting = "https://web1.karlsruhe.de/ris/oparl/bodies/0001/meetings"
link_agenda_item = "https://web1.karlsruhe.de/ris/oparl/bodies/0001/agendaItems"
link_consultation = "https://web1.karlsruhe.de/ris/oparl/bodies/0001/consultations"
link_paper = "https://web1.karlsruhe.de/ris/oparl/bodies/0001/papers"
link_file = "https://web1.karlsruhe.de/ris/oparl/bodies/0001/files"
link_location = "https://web1.karlsruhe.de/ris/oparl/bodies/0001/locations"
link_legislative_term = "https://web1.karlsruhe.de/ris/oparl/bodies/0001/legislativeTerms"

cur = conn.cursor()

# Funktion zum Erstellen der Tabelle, falls nicht vorhanden
def create_tables():
    cur.execute("""
    CREATE TABLE IF NOT EXISTS person (
        person_id VARCHAR(255) PRIMARY KEY,
        type_id VARCHAR(255) NOT NULL,
        body_id VARCHAR(255) NOT NULL,
        name VARCHAR(255),
        familyName VARCHAR(255),
        givenName VARCHAR(255),
        formOfAddress VARCHAR(255),
        affix VARCHAR(255),
        title TEXT[],
        gender VARCHAR(50),
        phone TEXT[],
        email TEXT[],
        location_id VARCHAR(255),
        status TEXT[],
        membership_id TEXT[],
        life TEXT,
        lifeSource VARCHAR(255),
        license VARCHAR(255),
        keyword TEXT[],
        created TIMESTAMPTZ,
        modified TIMESTAMPTZ,
        web VARCHAR(255),
        deleted BOOLEAN DEFAULT FALSE
    );
    CREATE TABLE IF NOT EXISTS membership (
        membership_id VARCHAR(255) PRIMARY KEY,
        type_id VARCHAR(255) NOT NULL,
        person_id VARCHAR(255),
        organization_id VARCHAR(255),
        role VARCHAR(255),
        votingRight BOOLEAN,
        startDate DATE,
        endDate DATE,
        onBehalfOf VARCHAR(255),
        license VARCHAR(255),
        keyword TEXT[],
        created TIMESTAMPTZ,
        modified TIMESTAMPTZ,
        web VARCHAR(255),
        deleted BOOLEAN DEFAULT FALSE
    );
    CREATE TABLE IF NOT EXISTS organization (
        organization_id VARCHAR(255) PRIMARY KEY,
        type_id VARCHAR(255) NOT NULL,
        body_id VARCHAR(255) NOT NULL,
        name VARCHAR(255),
        membership_id TEXT[],
        meetings_of_organization_id VARCHAR(255),
        consultation_id TEXT[],
        shortName VARCHAR(255),
        post TEXT[],
        subOrganizationOfUrl VARCHAR(255),
        organizationType VARCHAR(255),
        classification VARCHAR(255),
        startDate DATE,
        endDate DATE,
        website VARCHAR(255),
        location_id VARCHAR(255),
        externalBody BOOLEAN,
        license VARCHAR(255),
        keyword TEXT[],
        created TIMESTAMPTZ,
        modified TIMESTAMPTZ,
        web VARCHAR(255),
        deleted BOOLEAN DEFAULT FALSE
    );
    CREATE TABLE IF NOT EXISTS meeting (
        meeting_id VARCHAR(255) PRIMARY KEY,
        type_id VARCHAR(255) NOT NULL,
        name VARCHAR(255),
        start TIMESTAMPTZ,
        "end" TIMESTAMPTZ,
        location_id VARCHAR(255),
        organization TEXT[],
        invitation_id VARCHAR(255),
        results_protocol_id VARCHAR(255),
        verbatim_protocol_id VARCHAR(255),
        auxiliary_file TEXT[],
        agenda_item TEXT[],
        created TIMESTAMPTZ,
        modified TIMESTAMPTZ,
        web VARCHAR(255),
        deleted BOOLEAN DEFAULT FALSE,
        meetingState VARCHAR(255),
        cancelled BOOLEAN,
        participant TEXT[],
        license VARCHAR(255),
        keyword TEXT[]
    );
    CREATE TABLE IF NOT EXISTS agenda_item (
        agenda_item_id VARCHAR(255) PRIMARY KEY,
        type_id VARCHAR(255) NOT NULL,
        meeting_id VARCHAR(255),
        number VARCHAR(50),
        "order" INTEGER,
        name VARCHAR(255),
        public BOOLEAN,
        consultation_id VARCHAR(255),
        result VARCHAR(255),
        resolution_text TEXT,
        resolution_file_id VARCHAR(255),
        auxiliary_file_id TEXT[],
        created TIMESTAMPTZ,
        modified TIMESTAMPTZ,
        keyword TEXT[],
        deleted BOOLEAN DEFAULT FALSE
    );
    CREATE TABLE IF NOT EXISTS consultation (
        consultation_id VARCHAR(255) PRIMARY KEY,
        type_id VARCHAR(255) NOT NULL,
        paper_id VARCHAR(255),
        agenda_item_id VARCHAR(255),
        meeting_id VARCHAR(255),
        organization_id TEXT[],
        authoritative BOOLEAN,
        role VARCHAR(255),
        created TIMESTAMPTZ,
        modified TIMESTAMPTZ,
        license VARCHAR(255),
        keyword TEXT[],
        web VARCHAR(255),
        deleted BOOLEAN DEFAULT FALSE
    );
    CREATE TABLE IF NOT EXISTS paper (
        paper_id VARCHAR(255) PRIMARY KEY,
        type_id VARCHAR(255) NOT NULL,
        body_id VARCHAR(255),
        name VARCHAR(255),
        reference VARCHAR(255),
        date DATE,
        paper_type VARCHAR(255),
        related_paper_id TEXT[],
        superordinated_paper_id TEXT[],
        subordinated_paper_id TEXT[],
        main_file_id VARCHAR(255),
        auxiliary_file_id TEXT[],
        location_id TEXT[],
        originator_person TEXT[],
        under_direction_of TEXT[],
        originator_organization TEXT[],
        consultation_id TEXT[],
        license VARCHAR(255),
        keyword TEXT[],
        created TIMESTAMPTZ,
        modified TIMESTAMPTZ,
        web VARCHAR(255),
        deleted BOOLEAN DEFAULT FALSE
    );
    CREATE TABLE IF NOT EXISTS file (
        file_id VARCHAR(255) PRIMARY KEY,
        type_id VARCHAR(255) NOT NULL,
        name VARCHAR(255),
        file_name VARCHAR(255),
        mime_type VARCHAR(255),
        date DATE,
        size INTEGER,
        sha1_checksum VARCHAR(255),
        sha512_checksum VARCHAR(255),
        text TEXT,
        access_url VARCHAR(255),
        download_url VARCHAR(255),
        external_service_url VARCHAR(255),
        master_file VARCHAR(255),
        derivative_file TEXT[],
        file_license VARCHAR(255),
        meeting_id TEXT[],
        agenda_item_id TEXT[],
        paper_id TEXT[],
        license VARCHAR(255),
        keyword TEXT[],
        created TIMESTAMPTZ,
        modified TIMESTAMPTZ,
        web VARCHAR(255),
        deleted BOOLEAN DEFAULT FALSE
    );                
    CREATE TABLE IF NOT EXISTS location (
        location_id VARCHAR(255) PRIMARY KEY,
        type_id VARCHAR(255) NOT NULL,
        description TEXT,
        geojson JSONB,
        street_address VARCHAR(255),
        room VARCHAR(255),
        postal_code VARCHAR(50),
        sub_locality VARCHAR(255),
        locality VARCHAR(255),
        bodies TEXT[],
        organizations_id TEXT[],
        persons_id TEXT[],
        meetings_id TEXT[],
        papers_id TEXT[],
        license VARCHAR(255),
        keyword TEXT[],
        created TIMESTAMPTZ,
        modified TIMESTAMPTZ,
        web VARCHAR(255),
        deleted BOOLEAN DEFAULT FALSE
);
    CREATE TABLE IF NOT EXISTS legislative_term (
        legislative_term_id VARCHAR(255) PRIMARY KEY,
        type_id VARCHAR(255) NOT NULL,
        body_id VARCHAR(255),
        name VARCHAR(255),
        start_date DATE,
        end_date DATE,
        license VARCHAR(255),
        keyword TEXT[],
        created TIMESTAMPTZ,
        modified TIMESTAMPTZ,
        web VARCHAR(255),
        deleted BOOLEAN DEFAULT FALSE
    );
                            
    """)
    conn.commit()
    print("Table created successfully")

# Funktion zum Einfügen von Personendaten
def insert_person_data(data):
    for person in data:
        cur.execute("""
        INSERT INTO person (
            person_id, type_id, body_id, name, familyName, givenName, formOfAddress, affix, title, gender, phone, email, location_id, status, membership_id, life, lifeSource, license, keyword, created, modified, web, deleted
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s::TEXT[], %s, %s::TEXT[], %s::TEXT[], %s, %s::TEXT[], %s::TEXT[], %s, %s, %s::TEXT[], %s, %s, %s, %s, %s
        ) ON CONFLICT (person_id) DO NOTHING
        """, (
            person.get('id'),
            person.get('type'),
            person.get('body'),
            person.get('name'),
            person.get('familyName'),
            person.get('givenName'),
            person.get('formOfAddress'),
            person.get('affix'),
            person.get('title'),
            person.get('gender'),
            person.get('phone'),
            person.get('email'),
            person.get('location'),
            person.get('status'),
            [m['id'] for m in person.get('membership', [])],
            person.get('life'),
            person.get('lifeSource'),
            person.get('license'),
            person.get('keyword'),
            person.get('created'),
            person.get('modified'),
            person.get('web'),
            person.get('deleted', False)
        ))
        
    conn.commit()

# Funktion zum Einfügen von Membership-Daten
def insert_membership_data(data):
    for membership in data:
        cur.execute("""
        INSERT INTO membership (
            membership_id, type_id, person_id, organization_id, role, votingRight, startDate, endDate, onBehalfOf, license, keyword, created, modified, web, deleted
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::TEXT[], %s, %s, %s, %s
        ) ON CONFLICT (membership_id) DO NOTHING
        """, (
            membership.get('id'),
            membership.get('type'),
            membership.get('person'),
            membership.get('organization'),
            membership.get('role'),
            membership.get('votingRight'),
            membership.get('startDate'),
            membership.get('endDate'),
            membership.get('onBehalfOf'),
            membership.get('license'),
            membership.get('keyword'),
            membership.get('created'),
            membership.get('modified'),
            membership.get('web'),
            membership.get('deleted', False)
        ))
    conn.commit()

# Funktion zum Einfügen von Organization-Daten
def insert_organization_data(data):
    for organization in data:
        cur.execute("""
        INSERT INTO organization (
            organization_id, type_id, body_id, name, membership_id, meetings_of_organization_id, consultation_id, shortName, post, subOrganizationOfUrl, organizationType, classification, startDate, endDate, website, location_id, externalBody, license, keyword, created, modified, web, deleted
        ) VALUES (
            %s, %s, %s, %s, %s::TEXT[], %s, %s::TEXT[], %s, %s::TEXT[], %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::TEXT[], %s, %s, %s, %s
        ) ON CONFLICT (organization_id) DO NOTHING
        """, (
            organization.get('id'),
            organization.get('type'),
            organization.get('body'),
            organization.get('name'),
            organization.get('membership'),
            organization.get('meeting'),
            organization.get('consultation'),
            organization.get('shortName'),
            organization.get('post'),
            organization.get('subOrganizationOfUrl'),
            organization.get('organizationType'),
            organization.get('classification'),
            organization.get('startDate'),
            organization.get('endDate'),
            organization.get('website'),
            organization.get('location', {}).get('id'),
            organization.get('externalBody'),
            organization.get('license'),
            organization.get('keyword'),
            organization.get('created'),
            organization.get('modified'),
            organization.get('web'),
            organization.get('deleted', False)
        ))    
    conn.commit()    

def insert_meeting_data(data):
    for meeting in data:
        cur.execute("""
        INSERT INTO meeting (
            meeting_id, type_id, name, start, "end", location_id, organization, invitation_id, results_protocol_id, verbatim_protocol_id, auxiliary_file, agenda_item, created, modified, web, deleted, meetingState, cancelled, participant, license, keyword
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s::TEXT[], %s, %s, %s, %s::TEXT[], %s::TEXT[], %s, %s, %s, %s, %s, %s, %s::TEXT[], %s, %s::TEXT[] 
        ) ON CONFLICT (meeting_id) DO NOTHING
        """, (
            meeting.get('id'),
            meeting.get('type'),
            meeting.get('name'),
            meeting.get('start'),
            meeting.get('end'),
            meeting.get('location', {}).get('id'),
            meeting.get('organization'),
            meeting.get('invitation', {}).get('id'),
            meeting.get('resultsProtocol', {}).get('id'),
            meeting.get('verbatimProtocol', {}).get('id'),
            [aux['id'] for aux in meeting.get('auxiliaryFile', [])],
            [item['id'] for item in meeting.get('agendaItem', [])],
            meeting.get('created'),
            meeting.get('modified'),
            meeting.get('web'),
            meeting.get('deleted', False),
            meeting.get('meetingState'),
            meeting.get('cancelled', False),
            [participant['id'] for participant in meeting.get('participant', [])],
            meeting.get('license'),
            meeting.get('keyword')
        ))
    conn.commit()

def insert_agenda_item_data(data):
    for item in data:
        cur.execute("""
        INSERT INTO agenda_item (
            agenda_item_id, type_id, meeting_id, number, "order", name, public, consultation_id, result, resolution_text, resolution_file_id, auxiliary_file_id, created, modified, keyword, deleted
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::TEXT[], %s, %s, %s::TEXT[], %s
        ) ON CONFLICT (agenda_item_id) DO NOTHING
        """, (
            item.get('id'),
            item.get('type'),
            item.get('meeting'),
            item.get('number'),
            item.get('order'),
            item.get('name'),
            item.get('public'),
            item.get('consultation'),
            item.get('result'),
            item.get('resolutionText'),
            item.get('resolutionFile', {}).get('id'),
            [aux['id'] for aux in item.get('auxiliaryFile', [])],
            item.get('created'),
            item.get('modified'),
            item.get('keyword'),
            item.get('deleted', False)
        ))
    conn.commit()

def insert_consultation_data(data):
    for consultation in data:
        cur.execute("""
        INSERT INTO consultation (
            consultation_id, type_id, paper_id, agenda_item_id, meeting_id, organization_id, authoritative, role, created, modified, license, keyword, web, deleted
        ) VALUES (
            %s, %s, %s, %s, %s, %s::TEXT[], %s, %s, %s, %s, %s, %s::TEXT[], %s, %s
        ) ON CONFLICT (consultation_id) DO NOTHING
        """, (
            consultation.get('id'),
            consultation.get('type'),
            consultation.get('paper'),
            consultation.get('agendaItem'),
            consultation.get('meeting'),
            consultation.get('organization'),
            consultation.get('authoritative', False),
            consultation.get('role'),
            consultation.get('created'),
            consultation.get('modified'),
            consultation.get('license'),
            consultation.get('keyword'),
            consultation.get('web'),
            consultation.get('deleted', False)
        ))
    conn.commit()

def insert_paper_data(data):
    for paper in data:
        cur.execute("""
        INSERT INTO paper (
            paper_id, type_id, body_id, name, reference, date, paper_type, related_paper_id, superordinated_paper_id, subordinated_paper_id, main_file_id, auxiliary_file_id, location_id, originator_person, under_direction_of, originator_organization, consultation_id, license, keyword, created, modified, web, deleted
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s::TEXT[], %s::TEXT[], %s::TEXT[], %s, %s::TEXT[], %s::TEXT[], %s::TEXT[], %s::TEXT[], %s::TEXT[], %s::TEXT[], %s, %s::TEXT[], %s, %s, %s, %s
        ) ON CONFLICT (paper_id) DO NOTHING
        """, (
            paper.get('id'),
            paper.get('type'),
            paper.get('body'),
            paper.get('name'),
            paper.get('reference'),
            paper.get('date'),
            paper.get('paperType'),
            paper.get('relatedPaper'),
            paper.get('superordinatedPaper'),
            paper.get('subordinatedPaper'),
            paper.get('mainFile', {}).get('id'),
            [aux['id'] for aux in paper.get('auxiliaryFile', [])],
            [loc['id'] for loc in paper.get('location', [])],
            paper.get('originatorPerson'),
            paper.get('underDirectionOf'),
            paper.get('originatorOrganization'),
            [loc['id'] for loc in paper.get('consultation', [])],
            paper.get('license'),
            paper.get('keyword'),
            paper.get('created'),
            paper.get('modified'),
            paper.get('web'),
            paper.get('deleted', False)
        ))
    conn.commit()

def insert_file_data(data):
    for file in data:
        # Anpassung der URLs
        access_url = file.get('accessUrl', '').replace('web1.karlsruhe.de/oparl', 'web1.karlsruhe.de/ris/oparl')
        download_url = file.get('downloadUrl', '').replace('web1.karlsruhe.de/oparl', 'web1.karlsruhe.de/ris/oparl')

        cur.execute("""
        INSERT INTO file (
            file_id, type_id, name, file_name, mime_type, date, size, sha1_checksum, sha512_checksum, text, access_url, download_url, external_service_url, master_file, derivative_file, file_license, meeting_id, agenda_item_id, paper_id, license, keyword, created, modified, web, deleted
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::TEXT[], %s, %s::TEXT[], %s::TEXT[], %s::TEXT[], %s, %s::TEXT[], %s, %s, %s, %s
        ) ON CONFLICT (file_id) DO NOTHING
        """, (
            file.get('id'),
            file.get('type'),
            file.get('name'),
            file.get('fileName'),
            file.get('mimeType'),
            file.get('date'),
            file.get('size'),
            file.get('sha1Checksum'),
            file.get('sha512Checksum'),
            file.get('text'),
            access_url,
            download_url,
            file.get('externalServiceUrl'),
            file.get('masterFile'),
            file.get('derivativeFile'),
            file.get('fileLicense'),
            file.get('meeting'),
            file.get('agendaItem'),
            file.get('paper'),
            file.get('license'),
            file.get('keyword'),
            file.get('created'),
            file.get('modified'),
            file.get('web'),
            file.get('deleted', False)
        ))
    conn.commit()

def insert_location_data(data):
    for location in data:
        cur.execute("""
        INSERT INTO location (
            location_id, type_id, description, geojson, street_address, room, postal_code, sub_locality, locality, bodies, organizations_id, persons_id, meetings_id, papers_id, license, keyword, created, modified, web, deleted
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::TEXT[], %s::TEXT[], %s::TEXT[], %s::TEXT[], %s::TEXT[], %s, %s::TEXT[], %s, %s, %s, %s
        ) ON CONFLICT (location_id) DO NOTHING
        """, (
            location.get('id'),
            location.get('type'),
            location.get('description'),
            json.dumps(location.get('geojson')), # Extraktion der einzelnen Werte aus dem JSON-Objekt evtl. nötig
            location.get('streetAddress'),
            location.get('room'),
            location.get('postalCode'),
            location.get('subLocality'),
            location.get('locality'),
            location.get('bodies'),
            location.get('organizations'),
            location.get('persons'),
            location.get('meetings'),
            location.get('papers'),
            location.get('license'),
            location.get('keyword'),
            location.get('created'),
            location.get('modified'),
            location.get('web'),
            location.get('deleted', False)
        ))
    conn.commit()

def insert_legislative_term_data(data):
    for term in data:
        cur.execute("""
        INSERT INTO legislative_term (
            legislative_term_id, type_id, body_id, name, start_date, end_date, license, keyword, created, modified, web, deleted
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s::TEXT[], %s, %s, %s, %s
        ) ON CONFLICT (legislative_term_id) DO NOTHING
        """, (
            term.get('id'),
            term.get('type'),
            term.get('body'),
            term.get('name'),
            term.get('startDate'),
            term.get('endDate'),
            term.get('license'),
            term.get('keyword'),
            term.get('created'),
            term.get('modified'),
            term.get('web'),
            term.get('deleted', False)
        ))
    conn.commit()


# Funktion zum Abrufen der Daten von der API
def fetch_data(url, insert_object_data):
    while url:
        response = requests.get(url)
        if response.status_code == 200:
            print(f"Fetching data from {url}")
            data = response.json()
            insert_object_data(data['data'])
            url = data['links'].get('next')
            if url:
                url = url.replace(".de/oparl", ".de/ris/oparl")
            else:
                print("No more pages to fetch.")

# Tabellen erstellen
create_tables()

# Daten abrufen und in die Datenbank einfügen
fetch_data(link_person, insert_person_data)
fetch_data(link_membership, insert_membership_data)
fetch_data(link_organization, insert_organization_data)
fetch_data(link_meeting, insert_meeting_data)
fetch_data(link_agenda_item, insert_agenda_item_data)
fetch_data(link_consultation, insert_consultation_data)
fetch_data(link_paper, insert_paper_data)
fetch_data(link_file, insert_file_data)
fetch_data(link_location, insert_location_data)
fetch_data(link_legislative_term, insert_legislative_term_data)

# Verbindung schließen
cur.close()
conn.close()
