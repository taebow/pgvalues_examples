import csv
from functools import wraps
from statistics import mean
from contextlib import contextmanager
from time import perf_counter
from abc import ABC, abstractmethod

from pgvalues_examples import data_files


class Timer:

    def __init__(self, it):
        self.result = {}
        self.step_it = it

    @contextmanager
    def __call__(self):
        start = perf_counter()
        yield
        self.result[next(self.step_it)] = perf_counter() - start

    @staticmethod
    def session(run_method):
        @wraps(run_method)
        def wrapper(*args):
            exp, filename, _ = args
            timer = Timer(iter(exp.steps))
            result = run_method(*args, timer)
            timer.result["total"] = sum(timer.result.values())
            exp.results[filename].append(timer.result)
            return result
        return wrapper


class Experiment(ABC):

    # To set in subclasses
    steps = None

    def __init__(self):
        self.results = {}

    @property
    def name(self):
        return type(self).__name__

    @staticmethod
    def get_tokens(filename):
        tokens = []
        with open(filename, "r") as f:
            reader = csv.reader(f, dialect=csv.unix_dialect)
            for token in reader:
                tokens.append(tuple(token))
        return tokens

    def run_all(self, nb=5, verbose=False):
        for filename, path in data_files.items():
            self.results[filename] = []
            tokens = type(self).get_tokens(path)
            verbose and print(f"run {self.name} on {filename}", end="  ")
            for i in range(nb):
                self.run(filename, tokens)
                verbose and print(".", end="")
            verbose and print("\n", end="")

    @staticmethod
    def summary(*experiments):
        print("--Summary--")
        filenames = list(data_files.keys())
        cols = ["experiment"] + filenames
        cols_width = [max(map(len, [exp.name for exp in experiments]))] + [len(f) for f in filenames]
        print(" ".join(f"{col:{cols_width[i]}}" for i, col in enumerate(cols)))
        for exp in experiments:
            totals = {}
            for f in filenames:
                totals[f] = mean([r["total"] for r in exp.results[f]])
            print(f"{exp.name:{cols_width[0]}}", end=" ")
            print(" ".join([f"{totals[f]:{cols_width[i+1]}.{3}}" for i, f in enumerate(filenames)]))
        print(" ")

    @abstractmethod
    def run(self, filename, tokens):
        ...

    def display_results(self):
        print(f"---{self.name}---")
        mean_results = {}
        for filename, result in self.results.items():
            mean_results[filename] = {}
            for step in self.steps:
                mean_results[filename][step] = mean([r[step] for r in result])
            mean_results[filename]["total"] = mean(r["total"] for r in result)
        cols = ["filename"] + self.steps + ["total"]
        col_width = [max(map(len, (self.results.keys())))] + [len(s) for s in cols[1:]]
        print(" ".join([f"{col:{col_width[i]}}" for i, col in enumerate(cols)]))
        for filename, mean_result in mean_results.items():
            print(f"{filename:{col_width[0]}}", end=" ")
            print(" ".join([f"{mean_result[s]:{col_width[i+1]}.{3}}" for i, s in enumerate(self.steps)]), end=" ")
            print(f"{mean_result['total']:{col_width[-1]}.{3}}")
        print(" ")
