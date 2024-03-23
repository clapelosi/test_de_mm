# Text processing ETL

# Introduzione
Processo che carica e trasforma dati da documenti di testo (pre-lavorati) presenti in una directory.
Devono essere generati due file di output nella cartella output_dir (da creare allo stesso livello di etl.py):
 - un file csv
 - un file di tipo avro con i dati serializzati

 Ogni record del file csv e del file avro deve contenere i seguenti campi:
    - doc_id: stringa con l'id del documento
    - content: stringa corrispondente al testo originale del documento
    - token_count: integer col conteggio di tutti i token del testo originale
    - unique_token_count: integer col conteggio di tutti i token unici del testo originale
    - cleaned_text: testo pulito dai numeri, dai caratteri di ritorno a capo e con un solo spazio tra tutti i token
    - un campo per ogni lettera dell'alfabeto con il conteggio di tutti i token del testo che iniziano per quella lettera.

 Un esempio di record di output dovrebbe essere:
    {
        'doc_id': '341435', 
        'content': '10 esperienze mozzafiato da vita\n 10 esperienze mozzafiato da vita\n 10 esperienze mozzafiato da vita\n 10 esperienze mozzafiato da vita\n vita breve pensano riuscire viverla pieno debbano esperienze estreme ricerca nuove avventure amate adrenalina non potete meno sperimentare situazioni pericolose sito giusto seguito riportata classifica 10 esperienze mozzafiato da vita\n', 
        'token_count': 50, 
        'unique_token_count': 28, 
        'cleaned_text': 'esperienze mozzafiato da vita esperienze mozzafiato da vita esperienze mozzafiato da vita esperienze mozzafiato da vita vita breve pensano riuscire viverla pieno debbano esperienze estreme ricerca nuove avventure amate adrenalina non potete meno sperimentare situazioni pericolose sito giusto seguito riportata classifica esperienze mozzafiato da vita', 
        'a_tokens': 3, 
        'b_tokens': 1, 
        'c_tokens': 1, 
        'd_tokens': 6, 
        'e_tokens': 7, 
        'f_tokens': 0, 
        'g_tokens': 1, 
        'h_tokens': 0, 
        'i_tokens': 0, 
        'l_tokens': 0, 
        'm_tokens': 6, 
        'n_tokens': 2, 
        'o_tokens': 0, 
        'p_tokens': 4, 
        'q_tokens': 0, 
        'r_tokens': 3, 
        's_tokens': 4, 
        't_tokens': 0, 
        'u_tokens': 0, 
        'v_tokens': 7, 
        'z_tokens': 0
    }

Ambiente:
  Suggerito python3 con pandas e fastavro

Esempio:
  $ python etl.py -i data/in_text/


# How to run demo
usage: etl.py [-h] -i INDIR

optional arguments:
  -h, --help            show this help message and exit
  -i INDIR, --indir INDIR
                        input dir, with files .txt, plain text, one for each
                        document
  --outfile_avro OUTFILE_AVRO
                        out file avro. Default:output.avro
  --outfile_csv OUTFILE_CSV
                        out file csv. Default:output.csv
`
Esempio
`python etl.py -i data/in_text/ 

# Requirements
Python3

# Obiettivo
Implementare la funzione 'etl' (ed eventuali funzioni accessorie), che per la demo deve scrivere i file di output nella cartella output_dir

```
def etl(indir, output_avro_file, output_csv_file):
     pass

```