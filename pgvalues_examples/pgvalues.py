from time import perf_counter
from sqlalchemy import and_, func, Column, String
from pgvalues_examples.db import Dictionnary, session
from pgvalues_examples.data_loader import get_tokens
from pgvalues_examples.utils import PGValues


def build_tokens_values(tokens):

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


if __name__ == '__main__':
    tokens = get_tokens("data/last_glacial_period", sort=True)

    t0 = perf_counter()

    print("Build query")
    tokens_values = build_tokens_values(tokens)
    t1 = perf_counter()

    print("Filter tokens")
    filtered_tokens = filter_tokens(tokens_values)
    t2 = perf_counter()

    print("Aggregate tokens")
    agg = agg_tokens(tokens_values)
    t3 = perf_counter()

    build_tokens_values = t1 - t0
    filter_tokens_time = t2 - t1
    agg_tokens_time = t3 - t2

    total_filter_time = build_tokens_values + filter_tokens_time
    total_agg_time = build_tokens_values + agg_tokens_time

    print(build_tokens_values, filter_tokens_time, agg_tokens_time)
    print(f"Total time filter : {total_filter_time}")
    print(f"Total agg filter : {agg_tokens_time}")
    print(len(tokens), len(filtered_tokens))
    print([len(aggt[2]) for aggt in agg])