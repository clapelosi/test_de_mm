import os
import re


############# 
# constants #
#############

italian_alphabet = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']

schema = {
    "type": "record",
    "name": "Documento",
    "fields": [
        {"name": "doc_id", "type": "string"},
        {"name": "content", "type": "string"},
        {"name": "token_count", "type": "int"},
        {"name": "unique_token_count", "type": "int"},
        {"name": "cleaned_text", "type": "string"},
        {"name": "a_tokens", "type": "int"},
        {"name": "b_tokens", "type": "int"},
        {"name": "c_tokens", "type": "int"},
        {"name": "d_tokens", "type": "int"},
        {"name": "e_tokens", "type": "int"},
        {"name": "f_tokens", "type": "int"},
        {"name": "g_tokens", "type": "int"},
        {"name": "h_tokens", "type": "int"},
        {"name": "i_tokens", "type": "int"},
        {"name": "l_tokens", "type": "int"},
        {"name": "m_tokens", "type": "int"},
        {"name": "n_tokens", "type": "int"},
        {"name": "o_tokens", "type": "int"},
        {"name": "p_tokens", "type": "int"},
        {"name": "q_tokens", "type": "int"},
        {"name": "r_tokens", "type": "int"},
        {"name": "s_tokens", "type": "int"},
        {"name": "t_tokens", "type": "int"},
        {"name": "u_tokens", "type": "int"},
        {"name": "v_tokens", "type": "int"},
        {"name": "z_tokens", "type": "int"}
    ]
}

#############
# functions #
#############

def count_start_with_letter(content, letter):
    '''
        This function take as parameters:
            - content: string , it is the the text sting to split in tokens
            - letter: string, it is a string containing the letter with we want to find the count of tokens stat with

            It returns the count: integer, of the tokens inside the contet that start with the letter given as parameter
    '''
    # tokens is the array containing the single token of the content 
    tokens = content.split()

    # we store in a new array of the tokens the onlt ones start with the given letter
    token_starts_with_letter = [token for token in tokens if token.startswith(letter)]

    # it return the len of this array, the count of tokens staring with the given letter
    return len(token_starts_with_letter) 


def generate_record(file_path):

    doc_id = os.path.basename(file_path).replace('.txt', '')

    with open(file_path, 'r') as file:
        
        content = file.read()
        token_count = len(content.split())
        unique_token_count = len(set(content.split()))
        cleaned_text = re.sub(r'\d+', '', content).replace('\n', ' ').replace('   ', ' ')

    record = {
        'doc_id': doc_id,
        'content': content,
        'token_count': token_count,
        'unique_token_count': unique_token_count,
        'cleaned_text': cleaned_text,
    }

    italian_alphabet = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']

    for letter in italian_alphabet:
        record[f'{letter}_tokens'] = count_start_with_letter(cleaned_text, letter)

    return record

# def etl(indir, output_avro_file, output_csv_file):

#     record_list = []

#     for fileName in os.listdir(indir):
        
#         if fileName.endswith('.txt'):

#             file_path = os.path.join(indir, fileName)
#             record = generate_record(file_path)
        
#         record_list.append(record)

#     with open('output.avro', 'wb') as out_avro:
#         fastavro.writer(output_avro_file, schema, record_list)

#     df = pd.DataFrame(record_list)
#     df.to_csv(output_csv_file, index=False)

#     pass
