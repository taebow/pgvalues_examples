import csv


def get_tokens(path):
    result = []
    with open(path, "r") as f:
        reader = csv.reader(f, dialect=csv.unix_dialect)
        for token in reader:
            result.append(tuple(token))
    return result


if __name__ == '__main__':
    tokens = get_tokens("data/actus_reus")
    print(tokens)
