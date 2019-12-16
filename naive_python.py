from time import perf_counter
from sqlalchemy import or_, and_
from db import Dictionnary, session
from data_loader import get_tokens


def query_tokens(tokens):
    conditions = [
        and_(
            Dictionnary.word == t[1],
            Dictionnary.pos == t[2]
        )
        for t in tokens
    ]

    query = session.query(
        Dictionnary
    ).filter(or_(*conditions))

    return query.all()


def filter_tokens(tokens, reference):
    result = []
    for tok in tokens:
        for ref in reference:
            if tok[1] == ref.word and tok[2] == ref.pos:
                result.append(tok)
    return result


if __name__ == '__main__':
    tokens = get_tokens("data/brownie_lake")
    print(len(tokens))

    t0 = perf_counter()

    reference = query_tokens(tokens)
    t1 = perf_counter()

    filtered_tokens = filter_tokens(tokens, reference)
    t2 = perf_counter()
    print(len(filtered_tokens))

    for w in set([t[1] for t in tokens]).difference([t[1] for t in filtered_tokens]):
        print(w)

    print(t2-t0, t1-t0, t2-t1)