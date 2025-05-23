Ablauf des Data Preprocessing Workflows:
1. Ausführen von "structured_data_extraction" um die strukturierten Daten über die Oparl Schnittstelle zu extrahieren und in PostgreSQL zu speichern
2. Ausführen von "structured_data_formatting" um eine Tabelle mit den relevanten Metadaten zu erstellen basierend auf den extrahierten Daten
3. Ausführen von "pdf_processing_with_metadata" um die PDF Dateien inkl. der Metadaten in das passende Format für den Council Information Assistant zu preprocessen
