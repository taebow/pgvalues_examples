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


def filter_tokens(pg_values):
    return session\
        .query(pg_values.c.text, pg_values.c.word, pg_values.c.pos)\
        .join(Dictionnary, and_(Dictionnary.word == pg_values.c.word, Dictionnary.pos == pg_values.c.pos))\
        .all()


def agg_tokens(pg_values):
    return session\
        .query(Dictionnary.word, Dictionnary.pos, func.array_agg(pg_values.c.text))\
        .join(pg_values, and_(Dictionnary.word == pg_values.c.word, Dictionnary.pos == pg_values.c.pos))\
        .group_by(Dictionnary.word, Dictionnary.pos)\
        .all()


class OptPGValuesFilter(Experiment):
    steps = ["build_values", "filter"]

    @Timer.session
    def run(self, filename, tokens, timer=None):
        with timer():
            pg_values = build_values(tokens)

        with timer():
            filter_tokens(pg_values)


class OptPGValuesAggreg(Experiment):
    steps = ["build_values", "aggreg"]

    @Timer.session
    def run(self, filename, tokens, timer=None):
        with timer():
            pg_values = build_values(tokens)

        with timer():
            agg_tokens(pg_values)
