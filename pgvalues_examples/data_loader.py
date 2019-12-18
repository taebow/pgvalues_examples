import csv


def get_tokens(path, sort=False):
    result = []
    with open(path, "r") as f:
        reader = csv.reader(f, dialect=csv.unix_dialect)
        for token in reader:
            result.append(tuple(token))
    if sort:
        return sorted(result, key=lambda t: (t[1], t[2]))
    else:
        return result


if __name__ == '__main__':
    tokens = get_tokens("data/actus_reus")
    print(tokens)
