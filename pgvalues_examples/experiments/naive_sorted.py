from sqlalchemy import or_, and_
from pgvalues_examples.db import Dictionnary, session
from pgvalues_examples.experiments import Experiment, Timer


def build_query(tokens):
    conditions = [
        and_(
            Dictionnary.word == word,
            Dictionnary.pos == pos
        )
        for word, pos in {(w, p) for _, w, p in tokens}
    ]

    return session\
        .query(Dictionnary.word, Dictionnary.pos)\
        .filter(or_(*conditions))\
        .order_by(Dictionnary.word, Dictionnary.pos)


def filter_tokens(tokens, references):
    result, i, j = [], 0, 0
    while i < len(tokens) and j < len(references):
        _, word, pos = tokens[i]
        ref_word, ref_pos = references[j]
        if word == ref_word and pos == ref_pos:
            result.append(tokens[i])
        if word < ref_word or (word == ref_word and pos <= ref_pos):
            i += 1
        else:
            j += 1
    return result


def agg_tokens(tokens, references):
    result, token_text_buffer, i, j = [], [], 0, 0
    while i < len(tokens) and j < len(references):
        text, word, pos = tokens[i]
        ref_word, ref_pos = references[j]
        if word == ref_word and pos == ref_pos:
            token_text_buffer.append(text)
        if word < ref_word or (word == ref_word and pos <= ref_pos):
            i += 1
        else:
            result.append((ref_word, ref_pos, token_text_buffer))
            j += 1
            token_text_buffer = []

    return result


class NaiveSortedFilter(Experiment):
    steps = ["sort_tokens", "build_query", "exec_query", "filter"]

    @Timer.session
    def run(self, filename, tokens, timer=None):

        with timer():
            sorted_tokens = sorted(tokens, key=lambda t: (t[1], t[2]))

        with timer():
            query = build_query(tokens)

        with timer():
            references = query.all()

        with timer():
            filter_tokens(sorted_tokens, references)


class NaiveSortedAggreg(Experiment):
    steps = ["sort_tokens", "build_query", "exec_query", "aggreg"]

    @Timer.session
    def run(self, filename, tokens, timer=None):
        with timer():
            sorted_tokens = sorted(tokens, key=lambda t: (t[1], t[2]))

        with timer():
            query = build_query(tokens)

        with timer():
            references = query.all()

        with timer():
            agg_tokens(sorted_tokens, references)
