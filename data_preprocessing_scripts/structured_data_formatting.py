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

cur = conn.cursor()

def create_tables():
    cur.execute("""
    CREATE TABLE IF NOT EXISTS formatted_data_v2_turmbergbahn_extended AS
    SELECT        
        'paper_file' AS file_type,
        file.file_name AS pdf_name,
        file.name AS file_title,
        file.access_url AS access_url,
        paper.reference AS reference,
        paper.paper_type AS paper_type,
        ARRAY_AGG(consultation.role) AS role,
        ARRAY_AGG(organization.name) AS organization_name,
        paper.name AS agenda_item,
        ARRAY_AGG(agenda_item.result) AS result,
        ARRAY_AGG(TO_TIMESTAMP(TO_CHAR(meeting.start, 'YYYY-MM-DD'), 'YYYY-MM-DD')::date) AS meeting_date,
        STRING_AGG(
            organization.name || ' (' || TO_CHAR(meeting.start, 'DD.MM.YYYY') || ')', ', '
        ) AS sitzungen
    FROM file
    LEFT JOIN paper ON file.paper_id[1] = paper.paper_id
    LEFT JOIN consultation ON paper.paper_id = consultation.paper_id
    LEFT JOIN organization ON consultation.organization_id[1] = organization.organization_id
    LEFT JOIN agenda_item ON consultation.agenda_item_id = agenda_item.agenda_item_id
    LEFT JOIN meeting ON agenda_item.meeting_id = meeting.meeting_id
    WHERE paper.paper_id IS NOT NULL
    GROUP BY file.file_name, file.name, file.access_url, paper.reference, paper.paper_type, paper.name

    UNION

    SELECT        
        'agenda_item_auxiliary_file' AS file_type,
        file.file_name AS pdf_name,
        file.name AS file_title,
        file.access_url AS access_url,
        NULL AS reference,
        NULL AS paper_type,
        NULL AS role,
        ARRAY_AGG(organization.name) AS organization_name,
        agenda_item.name AS agenda_item,
        ARRAY_AGG(agenda_item.result) AS result,
        ARRAY_AGG(TO_TIMESTAMP(TO_CHAR(meeting.start, 'YYYY-MM-DD'), 'YYYY-MM-DD')::date) AS meeting_date,
        STRING_AGG(
            organization.name || ' (' || TO_CHAR(meeting.start, 'DD.MM.YYYY') || ')', ', '
        ) AS sitzungen
    FROM file
    LEFT JOIN agenda_item ON file.file_id = ANY(agenda_item.auxiliary_file_id)  -- Array-Join
    LEFT JOIN meeting ON agenda_item.meeting_id = meeting.meeting_id
    LEFT JOIN organization ON meeting.organization[1] = organization.organization_id
    WHERE agenda_item.auxiliary_file_id IS NOT NULL
    GROUP BY file.file_name, file.name, file.access_url, agenda_item.name
    
    UNION

    SELECT        
        'meeting_auxiliary_file' AS file_type,
        file.file_name AS pdf_name,
        file.name AS file_title,
        file.access_url AS access_url,
        NULL AS reference,
        NULL AS paper_type,
        NULL AS role,
        ARRAY_AGG(organization.name) AS organization_name,
        NULL AS agenda_item,
        NULL AS result,
        ARRAY_AGG(TO_TIMESTAMP(TO_CHAR(meeting.start, 'YYYY-MM-DD'), 'YYYY-MM-DD')::date) AS meeting_date,
        STRING_AGG(
            organization.name || ' (' || TO_CHAR(meeting.start, 'DD.MM.YYYY') || ')', ', '
        ) AS sitzungen
    FROM file
    LEFT JOIN meeting ON file.file_id = ANY(meeting.auxiliary_file)  -- Array-Join
    LEFT JOIN organization ON meeting.organization[1] = organization.organization_id
    WHERE meeting.auxiliary_file IS NOT NULL 
    GROUP BY file.file_name, file.name, file.access_url
    
    UNION

    SELECT        
        'verbatim_protocol_file' AS file_type,
        file.file_name AS pdf_name,
        file.name AS file_title,
        file.access_url AS access_url,
        NULL AS reference,
        NULL AS paper_type,
        NULL AS role,
        ARRAY_AGG(organization.name) AS organization_name,
        NULL AS agenda_item,
        NULL AS result,
        ARRAY_AGG(TO_TIMESTAMP(TO_CHAR(meeting.start, 'YYYY-MM-DD'), 'YYYY-MM-DD')::date) AS meeting_date,
        STRING_AGG(
            organization.name || ' (' || TO_CHAR(meeting.start, 'DD.MM.YYYY') || ')', ', '
        ) AS sitzungen
    FROM file
    LEFT JOIN meeting ON file.file_id = meeting.verbatim_protocol_id  -- Array-Join
    LEFT JOIN organization ON meeting.organization[1] = organization.organization_id
    WHERE meeting.verbatim_protocol_id IS NOT NULL 
    GROUP BY file.file_name, file.name, file.access_url

    UNION

    SELECT        
        'results_protocol_file' AS file_type,
        file.file_name AS pdf_name,
        file.name AS file_title,
        file.access_url AS access_url,
        NULL AS reference,
        NULL AS paper_type,
        NULL AS role,
        ARRAY_AGG(organization.name) AS organization_name,
        NULL AS agenda_item,
        NULL AS result,
        ARRAY_AGG(TO_TIMESTAMP(TO_CHAR(meeting.start, 'YYYY-MM-DD'), 'YYYY-MM-DD')::date) AS meeting_date,
        STRING_AGG(
            organization.name || ' (' || TO_CHAR(meeting.start, 'DD.MM.YYYY') || ')', ', '
        ) AS sitzungen
    FROM file
    LEFT JOIN meeting ON file.file_id = meeting.results_protocol_id  -- Array-Join
    LEFT JOIN organization ON meeting.organization[1] = organization.organization_id
    WHERE meeting.results_protocol_id IS NOT NULL 
    GROUP BY file.file_name, file.name, file.access_url
    
    UNION

    SELECT        
        'invitation_file' AS file_type,
        file.file_name AS pdf_name,
        file.name AS file_title,
        file.access_url AS access_url,
        NULL AS reference,
        NULL AS paper_type,
        NULL AS role,
        ARRAY_AGG(organization.name) AS organization_name,
        NULL AS agenda_item,
        NULL AS result,
        ARRAY_AGG(TO_TIMESTAMP(TO_CHAR(meeting.start, 'YYYY-MM-DD'), 'YYYY-MM-DD')::date) AS meeting_date,
        STRING_AGG(
            organization.name || ' (' || TO_CHAR(meeting.start, 'DD.MM.YYYY') || ')', ', '
        ) AS sitzungen
    FROM file
    LEFT JOIN meeting ON file.file_id = meeting.invitation_id  -- Array-Join
    LEFT JOIN organization ON meeting.organization[1] = organization.organization_id
    WHERE meeting.invitation_id IS NOT NULL
    GROUP BY file.file_name, file.name, file.access_url

    """)
    conn.commit()
    print("Table created successfully")


def create_primary_key():
     # Primärschlüssel zur neuen Tabelle hinzufügen
    cur.execute("""
        ALTER TABLE formatted_data 
        ADD CONSTRAINT formatted_data_pk PRIMARY KEY (pdf_name)
    """)
    conn.commit()
    print("Primärschlüssel für 'formatted_data' hinzugefügt.")


# Tabelle erstellen
create_tables()
# create_primary_key