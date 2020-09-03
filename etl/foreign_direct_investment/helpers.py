
import unicodedata


def norm(string):
    return unicodedata.normalize('NFKD', string).encode('ASCII', 'ignore').decode("utf-8")

def binarice_value(value):
    try:
        if float(value):
            return 0
    except:
        return 1