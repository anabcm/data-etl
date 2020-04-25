import unicodedata

def norm(string):
    return unicodedata.normalize('NFKD', string).encode('ASCII', 'ignore').decode("latin-1")