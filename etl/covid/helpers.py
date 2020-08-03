import unicodedata

def norm(string):
    try:
        return unicodedata.normalize('NFKD', string).encode('ASCII', 'ignore').decode("latin-1")
    except:
        return string