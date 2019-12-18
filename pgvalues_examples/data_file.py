import csv
import spacy
import requests
from justext import justext, get_stoplist

nlp = spacy.load('en_core_web_sm')

urls = [
    "https://en.wikipedia.org/wiki/Tibetan_Plateau",
    "https://en.wikipedia.org/wiki/Actus_reus"
]


def get_html(url):
    return url.split("/")[-1].lower(), requests.get(url).text


def get_text(html):
    return "\n".join(
        [
            p.text
            for p in justext(html, stoplist=get_stoplist("English"))
            if not p.is_boilerplate
        ]
    )


def get_tokens(text):
    return [
        (t.text, t.lemma_, t.pos_)
        for t in nlp(text)
        if t.pos_ in ["VERB", "NOUN", "ADJ", "ADV"]
    ]


def write_tokens(name, tokens):
    with open(f"data/{name}", "w") as f:
        writer = csv.writer(f, dialect=csv.unix_dialect)
        writer.writerow(["text", "lemma", "pos"])
        for token in tokens:
            writer.writerow(token)


if __name__ == '__main__':
    for url in urls:
        filename, html_content = get_html(url)
        text_content = get_text(html_content)
        toks = get_tokens(text_content)
        write_tokens(filename, toks)
