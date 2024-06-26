def format_person_doc(doc: str):
    # ex.: "13577891815.0" - remove .0
    if doc.endswith(".0"):
        doc = doc[:-2]     
    # remove dots and hyphens
    new_doc = ''.join(filter(str.isdigit, str(doc)))
    return new_doc
        
def verify_cpf_cnpj(doc: str):
    doc = format_person_doc(doc)
    if len(doc) <= 11:
        return "CPF"
    elif len(doc) > 11:
        return "CNPJ"

def need_update_doc(doc: str):
    special_characters = ['.', '/', '-']
    return doc.endswith(".0") or any(char in doc for char in special_characters)

def has_11(doc: str):
    doc = format_person_doc(doc)
    return len(doc) == 11

def has_14(doc: str):
    doc = format_person_doc(doc)
    return len(doc) == 14

def format_zero_left(doc: str):
    doc = format_person_doc(doc)
    if len(doc) < 11:        
        return doc.zfill(11)
    elif len(doc) > 11 and len(doc) < 14:
        return doc.zfill(14)
    return doc    