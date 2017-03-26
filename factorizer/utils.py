def random_ordered_sample(random_generator, population, k):
    """
    Computes a sample of k elements from a population list, maintaining the original order.

    :param random_generator: the random generator
    :type random_generator: random.Random

    :param population: the list to sample
    :type population: list

    :param k: the number of elements to select
    :type k: int

    :return: the sample list
    :rtype: list
    """
    indices_sample = random_generator.sample(range(len(population)), k)
    return [population[x] for x in sorted(indices_sample)]
