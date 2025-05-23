import os
import uuid
import psycopg
from dotenv import load_dotenv
from typing import Any
from pydantic import BaseModel

# unstructured
from unstructured.partition.pdf import partition_pdf

# langchain (or your library equivalents)
from langchain_core.documents import Document
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_postgres import PGVector
from langchain_postgres.vectorstores import PGVector
from langchain.retrievers.multi_vector import MultiVectorRetriever
from langchain_core.stores import InMemoryStore

# your custom imports
from backend.doc_store import PostgresByteStore

load_dotenv()

# Database connection
CONNECTION_STRING = (
    f"postgresql+psycopg://{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/production_turmbergbahn_large_embed_metadata"
)

# Connection for the database with additional structured metadata
# CONNECTION_STRING_STRUCTURED = (
#     f"postgresql+psycopg://{os.getenv('DB_USER')}:"
#     f"{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/ratsinformationssystem_structured_250129"
# )
structured_conn = psycopg.connect(
    dbname="ratsinformationssystem_structured_250129",
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    host="localhost",
    port="5432"
)

structured_cursor = structured_conn.cursor()

# Folder containing PDFs
path = r"C:\Users\mamac\Documents\Uni\8_Semester\Bachelorarbeit\Coding\Evaluation_Data\pdf_files\turmbergbahn_merged"

# Define a helper model (if needed)
class Element(BaseModel):
    type: str
    text: Any

# Initialize model and embeddings only once (outside the loop)
model = ChatOpenAI(temperature=0, model="gpt-4o-mini")
embeddings = OpenAIEmbeddings(model="text-embedding-3-large")

# Create Vector Store and Doc Store once
vector_store = PGVector(
    embeddings=embeddings,
    collection_name="summaries",
    connection=CONNECTION_STRING
)
store = PostgresByteStore(CONNECTION_STRING, "original_chunks")

# Create the MultiVectorRetriever
id_key = "doc_id"
retriever = MultiVectorRetriever(
    vectorstore=vector_store,
    docstore=store,
    id_key=id_key,
    search_type="similarity",
)

# Prepare the summarization chain
prompt_text = """
Du bist ein Assistent, der Tabellen und Texte unter Berücksichtigung von Metadaten zusammenfassen soll. Die Daten stammen aus einem Ratsinformationssystem.
Du kannst folgenden Metadaten als Kontext für die Zusammenfassung verwenden. Falls eine Variable nicht verfügbar oder NULL ist, vernachlässige sie einfach.:

- Titel des Dokuments: {file_title}
- Referenz: {reference}
- Dokumenttyp: {paper_type}
- Rolle: {role}
- Organisation: {organization_name}
- Tagesordnungspunkt: {agenda_item}
- Ergebnis: {result}
- Sitzungsdatum: {meeting_date}
- Sitzungen: {sitzungen}
- Dateityp: {file_type}

Hier ist der zu verarbeitende Inhalt:

{element}

Gib eine kurze Zusammenfassung basierend auf dem Inhalt und den Metadaten.
Antworte nur mit der Zusammenfassung, ohne zusätzlichen Kommentar.
Beginne deine Nachricht nicht mit den Worten „Hier ist eine Zusammenfassung“ oder ähnlichem.
Die Länge sollte optimalerweise 8 Sätze sein. Sollte der Text oder die Tabelle weniger Informationen enthalten, dann gib einfach so viele Sätze wieder, wie nötig sind.
"""
prompt = ChatPromptTemplate.from_template(prompt_text)
summarize_chain = prompt | model | StrOutputParser()

# Get a list of all PDF files in the directory
pdf_files = [f for f in os.listdir(path) if f.lower().endswith(".pdf")]

print("Starting to process PDFs...")

for pdf_file in pdf_files:
    pdf_path = os.path.join(path, pdf_file)
    print(f"\nProcessing file: {pdf_file}")

    # Partition/Chunk the PDF
    chunks = partition_pdf(
        filename=pdf_path,
        extract_images_in_pdf=False,
        infer_table_structure=True,
        chunking_strategy="by_title",
        strategy="hi_res",
        max_characters=4000,
        new_after_n_chars=3800,
        combine_text_under_n_chars=2000,
        image_output_dir_path=path,
    )
    
    # Fetch additional metadata from the structured database
    structured_cursor.execute(
        """
        SELECT file_title, reference, paper_type, role, organization_name, 
               agenda_item, result, meeting_date, sitzungen, file_type
        FROM formatted_data_v2_turmbergbahn_extended
        WHERE pdf_name = %s
        LIMIT 1
        """,
        (pdf_file,)  # oder (pdf_file.replace('.pdf', ''),) wenn in DB ohne .pdf gespeichert
    )
    structured_row = structured_cursor.fetchone()

    # structured_row kann None sein, wenn kein Treffer in DB
    file_title = None
    reference = None
    paper_type = None
    role = None
    organization_name = None
    agenda_item = None
    result = None
    meeting_date = None

    if structured_row:
        (
            file_title, 
            reference, 
            paper_type, 
            role, 
            organization_name, 
            agenda_item, 
            result, 
            meeting_date,
            sitzungen,
            file_type
        ) = structured_row

    # Explicitly ensure all values are strings
    # metadata = {
    #     "file_title": str(file_title),
    #     "reference": str(reference),
    #     "paper_type": str(paper_type),
    #     "role": str(role),
    #     "organization_name": str(organization_name),
    #     "agenda_item": str(agenda_item),
    #     "result": str(result),
    #     "meeting_date": str(meeting_date),
    #     "sitzungen": str(sitzungen),
    #     "file_type": str(file_type),
    # }
    # # Debugging print to check metadata before summarization
    # print("\nDEBUG: Metadata for", pdf_file)
    # print(metadata)


    # Set a synthetic URL (if needed) for each chunk
    for chunk in chunks:
        # Adjust URL, or store any metadata you want
        chunk.metadata.url = (
            "https://web1.karlsruhe.de/ris/oparl/bodies/0001/downloadfiles/a/"
            f"{pdf_file}"
        )
        # Also keep track of the actual filename for reference
        chunk.metadata.filename = pdf_file

    # Separate tables from texts
    tables, texts = [], []
    for chunk in chunks:
        chunk_type = str(type(chunk))
        if "Table" in chunk_type:
            tables.append(chunk)
        elif "CompositeElement" in chunk_type:
            texts.append(chunk)

    # Summarize text chunks
    if texts:
        text_summaries = summarize_chain.batch(
            [
                {
                    "element": text.text,
                    "file_title": file_title or "",
                    "reference": reference or "",
                    "paper_type": paper_type or "",
                    "role": role or "",
                    "organization_name": organization_name or "",
                    "agenda_item": agenda_item or "",
                    "result": result or "",
                    "meeting_date": meeting_date or "",
                    "sitzungen": sitzungen or "",
                    "file_type": file_type or "",
                }
                for text in texts
            ],
            {"max_concurrency": 3}  # Tuning je nach Bedarf
        )
    else:
        text_summaries = []


    # Summarize table chunks
    if tables:
        table_summaries = summarize_chain.batch(
            [
                {
                    "element": table.metadata.text_as_html,
                    "file_title": file_title or "",
                    "reference": reference or "",
                    "paper_type": paper_type or "",
                    "role": role or "",
                    "organization_name": organization_name or "",
                    "agenda_item": agenda_item or "",
                    "result": result or "",
                    "meeting_date": meeting_date or "",
                    "sitzungen": sitzungen or "",
                    "file_type": file_type or "",
                }
                for table in tables
            ],
            {"max_concurrency": 3}
        )
    else:
        table_summaries = []


    # Convert text chunks into Documents
    text_documents = [
        Document(
            page_content=text.text,
            metadata={
                "url": text.metadata.url,
                "filename": text.metadata.filename,
                "file_title": file_title,
                "reference": reference,
                "paper_type": paper_type,
                "role": role,
                "organization_name": organization_name,
                "agenda_item": agenda_item,
                "result": result,
                "meeting_date": meeting_date,
                "sitzungen": sitzungen,
                "file_type": file_type,
            },
        )
        for text in texts
    ]

    # Convert table chunks into Documents
    table_documents = [
        Document(
            page_content=table.metadata.text_as_html,
            metadata={
                "url": table.metadata.url,
                "filename": table.metadata.filename,
                "file_title": file_title,
                "reference": reference,
                "paper_type": paper_type,
                "role": role,
                "organization_name": organization_name,
                "agenda_item": agenda_item,
                "result": result,
                "meeting_date": meeting_date,
                "sitzungen": sitzungen,
                "file_type": file_type,                
            },
        )
        for table in tables
    ]

    # Insert summarized texts into the vector store + doc store
    if text_documents:
        doc_ids = [str(uuid.uuid4()) for _ in text_documents]
        summary_text_docs = [
            Document(page_content=summary, metadata={id_key: doc_ids[i]})
            for i, summary in enumerate(text_summaries)
        ]
        # Add summary vectors
        retriever.vectorstore.add_documents(summary_text_docs)
        # Link doc IDs to original text documents
        retriever.docstore.mset(
            list(zip(doc_ids, text_documents, ["test"] * len(doc_ids)))
        )

    # Insert summarized tables into the vector store + doc store
    if table_documents:
        table_ids = [str(uuid.uuid4()) for _ in table_documents]
        summary_table_docs = [
            Document(page_content=summary, metadata={id_key: table_ids[i]})
            for i, summary in enumerate(table_summaries)
        ]
        # Add summary vectors
        retriever.vectorstore.add_documents(summary_table_docs)
        # Link doc IDs to original table documents
        retriever.docstore.mset(
            list(zip(table_ids, table_documents, ["test"] * len(table_ids)))
        )

    print(f"Finished processing: {pdf_file}")


# Bei größeren Datenmengen hier noch einen Index über die Vektoren erzeugen 



structured_cursor.close()
structured_conn.close()

print("All PDFs processed.")
