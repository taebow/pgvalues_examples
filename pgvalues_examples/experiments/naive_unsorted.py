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
        .filter(or_(*conditions))


def filter_tokens(tokens, references):
    result = []
    for tok in tokens:
        _, word, pos = tok
        for ref in references:
            ref_word, ref_pos = ref
            if word == ref_word and pos == ref_pos:
                result.append(tok)
    return result


def agg_tokens(tokens, references):
    result = []
    for ref in references:
        output_row = (ref.word, ref.pos, [])
        ref_word, ref_pos, text_tokens = output_row
        for tok in tokens:
            text, word, pos = tok
            if word == ref_word and pos == ref_pos:
                text_tokens.append(text)
        result.append(output_row)
    return result


class NaiveUnsortedFilter(Experiment):
    steps = ["build_query", "exec_query", "filter"]

    @Timer.session
    def run(self, filename, tokens, timer=None):
        with timer():
            query = build_query(tokens)

        with timer():
            references = query.all()

        with timer():
            filter_tokens(tokens, references)


class NaiveUnsortedAggreg(Experiment):
    steps = ["build_query", "exec_query", "aggreg"]

    @Timer.session
    def run(self, filename, tokens, timer=None):
        with timer():
            query = build_query(tokens)

        with timer():
            references = query.all()

        with timer():
            agg_tokens(tokens, references)
