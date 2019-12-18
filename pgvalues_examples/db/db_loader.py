from nltk.corpus import wordnet as wn

from pgvalues_examples.db import Dictionnary, session


def convert_pos(wn_pos):
    return {
        "n": "NOUN",
        "v": "VERB",
        "a": "ADJ",
        "r": "ADV",
        "s": "ADJ"
    }[wn_pos]


def seed_wordnet(word_pos):
    session.bulk_insert_mappings(
        Dictionnary, word_pos
    )
    session.commit()


if __name__ == '__main__':
    Dictionnary.metadata.create_all(session.bind)

    lemmas = [w for w in wn.all_lemma_names()]
    word_pos = []
    for word in lemmas:
        for pos in set([convert_pos(syn.pos()) for syn in wn.synsets(word)]):
            word_pos.append({"word": word, "pos": pos})

    seed_wordnet(word_pos)
