from sqlalchemy import and_, func, Column, String
from pgvalues_examples.db import Dictionnary, session
from pgvalues_examples.db.utils import PGValues
from pgvalues_examples.experiments import Experiment, Timer


def build_values(tokens):

    return PGValues(
        [
            Column("text", String),
            Column("word", String),
            Column("pos", String),
        ],
        *tokens,
        alias_name="tokens",
    )


def filter_tokens(tokens):
    return session\
        .query(tokens.c.text, tokens.c.word, tokens.c.pos)\
        .join(Dictionnary, and_(Dictionnary.word == tokens.c.word, Dictionnary.pos == tokens.c.pos))\
        .all()


def agg_tokens(tokens):
    return session\
        .query(Dictionnary.word, Dictionnary.pos, func.array_agg(tokens.c.text))\
        .join(tokens, and_(Dictionnary.word == tokens.c.word, Dictionnary.pos == tokens.c.pos))\
        .group_by(Dictionnary.word, Dictionnary.pos)\
        .all()


class OptPGValuesFilter(Experiment):
    steps = ["build_values", "filter"]

    @Timer.session
    def run(self, filename, tokens, timer=None):
        with timer():
            tok_values = build_values(tokens)

        with timer():
            filter_tokens(tok_values)


class OptPGValuesAggreg(Experiment):
    steps = ["build_values", "aggreg"]

    @Timer.session
    def run(self, filename, tokens, timer=None):
        with timer():
            tok_values = build_values(tokens)

        with timer():
            agg_tokens(tok_values)
