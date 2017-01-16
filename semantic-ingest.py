import os

import spacy                         # See "Installing spaCy"
nlp = spacy.load('en')               # You are here.
doc = nlp(u'Hello, spacy!')          # See "Using the pipeline"
print((w.text, w.pos_) for w in doc) # See "Doc, Span and Token"

# 