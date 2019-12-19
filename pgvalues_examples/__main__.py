from pgvalues_examples.experiments import *


if __name__ == '__main__':
    experiments = [
        NaiveUnsortedFilter(),
        NaiveSortedFilter(),
        OptPGValuesFilter(),
        NaiveUnsortedAggreg(),
        NaiveSortedAggreg(),
        OptPGValuesAggreg()
    ]

    for exp in experiments:
        exp.run_all(nb=50, verbose=True)

    for exp in experiments:
        exp.display_results()

    Experiment.summary(*experiments)
