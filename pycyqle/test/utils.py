import re

def format_query(query):
    return re.sub(r'\s?,\s?', ',', ' '.join(query.split()))