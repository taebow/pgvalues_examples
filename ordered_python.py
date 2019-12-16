from time import perf_counter
from sqlalchemy import or_, and_, func
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
        Dictionnary.word, Dictionnary.pos
    ).filter(
        or_(*conditions)
    ).order_by(
        Dictionnary.word,
        Dictionnary.pos
    )

    return query.all()


def filter_tokens(tokens, references):
    tokens = sorted(tokens, key=lambda t: (t[1], t[2]))
    result, i, j = [], 0, 0
    while i < len(tokens) and j < len(references):
        _, word, pos, ref_word, ref_pos = (*tokens[i], *references[j])
        if word == ref_word:
            if pos == ref_pos:
                result.append(tokens[i])
            if pos <= ref_pos:
                i += 1
            else:
                j += 1
        elif word < ref_word:
            i += 1
        else:
            j += 1

    return result

    # for tok in tokens:
    #     word = tok[1]
    #     pos = tok[2]
    #     for j in range(i, len(reference)):
    #         ref_word = reference[j].word
    #         ref_pos = reference[j].pos
    #         if word == ref_word and pos == ref_pos:
    #             result.append(tok)
    #             i = j
    #             break
    #         elif word < ref_word \
    #                 or word == ref_word and pos < ref_pos:
    #             i = j
    #             break
    #         else:
    #             continue
    #
    # return result


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