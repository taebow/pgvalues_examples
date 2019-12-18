from time import perf_counter
from sqlalchemy import or_, and_
from pgvalues_examples.db import Dictionnary, session
from pgvalues_examples.data_loader import get_tokens


def build_query(tokens):
    conditions = [
        and_(
            Dictionnary.word == word,
            Dictionnary.pos == pos
        )
        for _, word, pos in tokens
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


if __name__ == '__main__':
    tokens = get_tokens("data/last_glacial_period")

    t0 = perf_counter()

    print("Build query")
    query = build_query(tokens)
    t1 = perf_counter()

    print("Exec query")
    references = query.all()
    t2 = perf_counter()

    print("Filter tokens")
    filtered_tokens = filter_tokens(tokens, references)
    t3 = perf_counter()

    print("Aggregate tokens")
    agg = agg_tokens(tokens, references)
    t4 = perf_counter()

    build_query_time = t1-t0
    exec_query_time = t2-t1
    filter_tokens_time = t3-t2
    agg_tokens_time = t4-t3

    total_filter_time = build_query_time + exec_query_time + filter_tokens_time
    total_agg_time = build_query_time + exec_query_time + agg_tokens_time

    print(build_query_time, exec_query_time, filter_tokens_time, agg_tokens_time)
    print(f"Total time filter : {total_filter_time}")
    print(f"Total agg filter : {agg_tokens_time}")
    print(len(tokens), len(filtered_tokens))
    print([len(aggt[2]) for aggt in agg])